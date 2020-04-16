import * as Redis from 'ioredis'
import { Cluster } from 'ioredis'
import { EventEmitter } from 'events'
import { EVENT } from '@deepstream/types'
import { URL} from 'url'

export class RedisConnection {
  private isReady: boolean = false
  private emitter = new EventEmitter()
  public client: Redis.Redis

  constructor (options: any, private logger: any) {
    this.validateOptions(options)

    if (options.url) {
      const REDIS_URL = new URL(options.url)
      options.host = REDIS_URL.hostname
      options.port = REDIS_URL.port
      options.password = REDIS_URL.password
    }

    if (options.nodes instanceof Array) {
      const nodes = options.nodes
      delete options.nodes
      this.client = new Cluster(nodes, options) as any
    } else {
      this.client = new Redis(options)
    }

    this.client.on('ready', this.onReady.bind(this))
    this.client.on('error', this.onError.bind(this))
    this.client.on('end', this.onDisconnect.bind(this))
  }

  public whenReady () {
    if (!this.isReady) {
      return new Promise((resolve) => this.emitter.once('ready', resolve))
    }
  }

  public close () {
    return new Promise((resolve) => {
      this.client.removeAllListeners('end')
      this.client.once('end', resolve)
      this.client.quit()
    })
  }

  private onReady () {
    this.isReady = true
    this.emitter.emit('ready')
  }

  private onError (error: string) {
    this.logger.fatal('REDIS_CONNECTION_ERROR', `REDIS error: ${error}`)
  }

  private onDisconnect (error: string) {
    this.onError('disconnected')
  }

  private validateOptions (options: any) {
    if (!options) {
      this.logger.fatal(EVENT.PLUGIN_INITIALIZATION_ERROR, "Missing option 'host' for redis-connector")
    }
    if (options.nodes && !(options.nodes instanceof Array)) {
      this.logger.fatal(EVENT.PLUGIN_INITIALIZATION_ERROR, 'Option nodes must be an array of connection parameters for cluster')
    }
  }
}
