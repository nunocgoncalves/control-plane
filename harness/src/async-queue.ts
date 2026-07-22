// A minimal bounded async queue shared by the work-client (bidi stream input)
// and the child-process event stream (HOR-381). Fowler "Duplicated Code"
// baseline: previously two near-identical copies lived in work-client.ts and
// child-process.ts. push() yields to a waiting consumer or buffers; close()
// ends the stream. throw()/return() (called by the Connect bidi client on
// cancel) close the queue so cancellation semantics are fixed in one place.

export class AsyncQueue<T> implements AsyncIterable<T> {
  private readonly buf: T[] = [];
  private closed = false;
  private readonly waiters: Array<(r: IteratorResult<T>) => void> = [];

  push(v: T): void {
    if (this.closed) return;
    const w = this.waiters.shift();
    if (w) w({ value: v, done: false });
    else this.buf.push(v);
  }

  close(): void {
    this.closed = true;
    for (const w of this.waiters) w({ value: undefined as never, done: true });
    this.waiters.length = 0;
  }

  get isClosed(): boolean {
    return this.closed;
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.buf.length) return Promise.resolve({ value: this.buf.shift() as T, done: false });
        if (this.closed) return Promise.resolve({ value: undefined as never, done: true });
        return new Promise((resolve) => this.waiters.push(resolve));
      },
      throw: async (): Promise<IteratorResult<T>> => {
        this.close();
        return { value: undefined as never, done: true };
      },
      return: async (): Promise<IteratorResult<T>> => {
        this.close();
        return { value: undefined as never, done: true };
      },
    };
  }
}
