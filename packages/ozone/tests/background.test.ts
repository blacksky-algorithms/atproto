import { BackgroundQueue, PeriodicBackgroundTask } from '../src/background.js'

describe('PeriodicBackgroundTask', () => {
  it('runs once more after an in-flight run when forced', async () => {
    const queue = new BackgroundQueue({} as any, { concurrency: 1 })
    const started: number[] = []
    let release = () => {}
    const task = new PeriodicBackgroundTask(queue, 60_000, async () => {
      started.push(started.length + 1)
      if (started.length === 1) {
        await new Promise<void>((resolve) => {
          release = resolve
        })
      }
    })

    const first = task.run()
    await new Promise((resolve) => setTimeout(resolve, 20))
    expect(started).toEqual([1])
    const forced = task.run()
    release()
    await first
    await forced
    expect(started).toEqual([1, 2])

    // nothing in flight: a forced run is a single run
    await task.run()
    expect(started).toEqual([1, 2, 3])
  })
})
