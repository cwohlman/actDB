type Log = {
  id: string,
  version: number,
}
type Row = Log & { value: VanillaValue }

type ActionLog = Log & { seq: number, args?: VanillaValue, action: Action<any> }
type ActionRow = ActionLog & Row;

type Action<T extends VanillaValue = VanillaValue> = (query: ActDB["query"], args: T) => VanillaValue;

type VanillaValue = string | number | boolean | null | (VanillaValue | undefined)[] | { [key: string]: VanillaValue | undefined };

type LogFilter = (log: Log, get: (id: string) => Row | null) => boolean;
type Query = string | number | LogFilter
type QueryOptions = { values?: boolean, all?: boolean, version?: number }

class ActDB {
  private log: Log[] = []
  private seqIndex: ActionLog[] = []
  private idIndex: { [id: string]: Log } = {}
  
  private values: { [id: string]: VanillaValue } = {}

  query(): ActionRow | null
  query(options: { version?: number }): ActionRow | null
  query(options: { values: false, version?: number }): ActionLog | null
  query(options: { values: true, version?: number }): ActionRow | null
  query(id: number): ActionRow | null
  query(id: number, options: { version?: number }): ActionRow | null
  query(id: number, options: { values: true, version?: number }): ActionRow | null
  query(id: number, options: { values: false, version?: number }): ActionLog | null

  query(id: string): Row | null
  query(id: string, options: { version?: number }): Row | null
  query(id: string, options: { values: true, version?: number }): Row | null
  query(id: string, options: { values: false, version?: number }): Log | null

  query(options: { all: true, version?: number }): Row[]
  query(options: { all: true, values: false, version?: number }): Log[]
  query(options: { all: true, values: true, version?: number }): Row[]

  query(filter: (log: Log, get: (id: string) => Row | null) => boolean): Row | null
  query(filter: (log: Log, get: (id: string) => Row | null) => boolean, options: { version?: number }): Row | null
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T): (T & Row) | null
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T, options: { version?: number }): (T & Row) | null
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T, options: { values: true, version?: number }): (T & Row) | null
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T, options: { values: false, version?: number }): T | null
  //query<T>(filter: (log: Log, get: (id: string) => Row | null) => T | null): T

  query(filter: (log: Log, get: (id: string) => Row | null) => boolean, options: { all: true, version?: number }): Row[]
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T, options: { all: true, version?: number }): (T & Row)[]
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T, options: { all: true, values: true, version?: number }): (T & Row)[]
  query<T extends Log>(filter: (log: Log, get: (id: string) => Row | null) => log is T, options: { all: true, values: false, version?: number }): T[]
  //query<T>(filter: (log: Log, get: (id: string) => Row | null) => T | null, options: { all: true }): T[]

  query(...args: Parameters<
    (() => void) |
    ((query: Query) => void) |  
    ((options: QueryOptions) => void) |  
    ((query: Query, options: QueryOptions) => void)   
  >): any {
    if (! args.length) return this.latest();

    if (typeof args[0] == "object") {
      const options = args[0];
      if (options.all) return this.all(options);
      return this.latest(options);
    }

    if (typeof args[0] == "string") {
      const id = args[0];
      const options = args[1];

      return this.getLog(id, options);
    }

    if (typeof args[0] == "number") {
      const id = args[0];
      const options = args[1];

      return this.getAction(id, options);
    }

    if (typeof args[0] == "function") {
      const filter = args[0];
      const options = args[1];

      return this.getLogs(filter, options);
    }

    throw new Error("Unrecognized query type: " + typeof args[0]);
  }

  at(version: number): ActDB["query"] {
    return (...args: Parameters<
      (() => void) |
      ((query: Query) => void) |  
      ((options: QueryOptions) => void) |  
      ((query: Query, options: QueryOptions) => void)   
    >): any => {
      if (! args.length) {
        return this.query({ version });
      }
      if (typeof args[0] == "object") {
        const minVersion = args[0].version ? Math.min(version, args[0].version) : version;
        return this.query({...args[0], version: minVersion })
      }
      if (typeof args[1] == "object") {
        const minVersion = args[1].version ? Math.min(version, args[1].version) : version;
        return this.query({...args[1], version: minVersion })
      }

      // I'm not quite sure why this doesn't pass the type signature
      return this.query(args[0] as any, {version});
    }
  }

  latest(options?: { values?: boolean, version?: number }): ActionLog | ActionRow | null {
    if (typeof options?.version == "number") {
      return this.getAction(this.seekAction(options.version), options);
    }

    const lastSeqNumber = this.seqIndex.length - 1;
    return this.getAction(lastSeqNumber, options);
  }

  seekAction(version: number) {
    for (var i = version; i >= 0; i--) {
      const logAction = this.log[i] as (Log | ActionLog);
      if ("seq" in logAction) {
        return logAction.seq;
      }
    }

    return -1;
  }

  getAction(id: number, options?: { values?: boolean, version?: number }) {
    const log = this.seqIndex[id];

    if (! log) {
      return null;
    }

    if (typeof options?.version == "number" && log.version > options.version) {
      return null;
    }

    if (options?.values != false) {
      return this.hydrate(log);
    }
    
    return log;
  }

  getLog(id: string, options?: { values?: boolean, version?: number }) {
    const log = this.idIndex[id];

    if (! log) {
      return null;
    }

    if (typeof options?.version == "number" && log.version > options.version) {
      return null;
    }

    if (options?.values != false) {
      return this.hydrate(log);
    }
    
    return log;
  }

  getLogs(filter: LogFilter, options?: { values?: boolean, version?: number }) {
    const version = options?.version || this.log.length - 1;
    const logs = this.log.filter((item) => {
      if (item.version > version) return false;

      return filter(item, id => this.query(id, { version }))
    });

    if (options?.values != false) {
      return logs.map(log => this.hydrate(log));
    }

    return logs;
  }

  all(options?: QueryOptions) {
    let logs = this.log;

    if (typeof options?.version == "number") {
      logs = logs.slice(0, options.version);
    }

    if (options?.values != false) {
      return logs.map(row => this.hydrate(row))
    }

    return logs.concat([]);
  }

  act<T extends VanillaValue>(action: Action<T>, args: T) {
    const log: ActionLog = {
      id: this.getId(),
      version: this.log.length,
      seq: this.seqIndex.length,
      args: args,
      action: action,
    }

    this.pushActionLog(log);

    return log;
  }

  store(value: VanillaValue) {
    const log: Log = {
      id: this.getId(),
      version: this.log.length,
    }

    this.values[log.id] = value;
    this.pushLog(log);

    return log.id;
  }

  lookup(id: string): Log {
    return this.idIndex[id];
  }

  hydrate<T extends Log>(log: T): T & Row {
    const value = this.resolve(log);

    return {
      ...log,
      value
    };
  }

  resolve(log: Log | ActionLog) {
    if (log.id in this.values) {
      return this.values[log.id];
    }

    if ('seq' in log) {
      const result = this.evaluate(log);

      this.values[log.id] = result;

      return result;
    }

    return null;
  }

  evaluate(log: ActionLog) {
    return log.action(this.at(log.version - 1), log.args);
  }

  private pushActionLog(log: ActionLog) {
    this.pushLog(log);
    this.seqIndex[log.seq] = log;
  }

  private pushLog(log: Log) {
    this.log.push(log);
    this.idIndex[log.id] = log;
  }

  getId() {
    return "id_" + this.log.length;
  }
}

const db = new ActDB();

const fooId = db.store("foo");

const barId = db.store({ "bar": 100 });

console.log(db.query(fooId))

const action = db.act((query, args) => {
  const foo = query(args.foo)?.value;
  const bar = query(args.bar)?.value;

  return {foo, bar};
}, { foo: fooId, bar: barId })

console.log(action);

console.log(db.query())

const action2 = db.act((query, args) => {
  const previousValue = query()?.value;
  if (previousValue && typeof previousValue == 'object' && previousValue instanceof Array) return previousValue.concat(args)
  
  
  return [args]
}, { biz: 1 })


const action3 = db.act((query, args) => {
  const previousValue = query()?.value;
  if (previousValue && typeof previousValue == 'object' && previousValue instanceof Array) return previousValue.concat(args)
  
  
  return [args]
}, { biz: 2 })

console.log(db.query());

console.log(db.query((a: any) => a.args?.biz == 2));