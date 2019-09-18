import * as pkg from '../package.json'
import { RedisOptions } from 'ioredis'
import { RedisConnection } from './redis-connection'
import { Message } from '@deepstream/protobuf/dist/types/messages'
import { TOPIC, STATE_REGISTRY_TOPIC } from '@deepstream/protobuf/dist/types/all'
import { DeepstreamPlugin, DeepstreamClusterNode, DeepstreamServices, DeepstreamConfig, EVENT } from '@deepstream/types'

export default class RedisClusterNode extends DeepstreamPlugin implements DeepstreamClusterNode {
    public description = `Redis Cluster Node Version: ${pkg.version}`
    private callbacks: Map<TOPIC | STATE_REGISTRY_TOPIC, Set<Function>>
    private connection: RedisConnection
    private subscribeConnection: RedisConnection

    constructor (pluginConfig: RedisOptions, private services: DeepstreamServices, private config: DeepstreamConfig) {
        super()
        this.callbacks = new Map()
        this.connection = new RedisConnection(pluginConfig, this.services.logger)
        this.subscribeConnection = new RedisConnection(pluginConfig, this.services.logger)
        this.subscribeConnection.client.on('message', this.onMessage.bind(this))
    }

    public async whenReady () {
        await this.connection.whenReady()
    }

    public async close () {
        await this.connection.close()
    }

    public sendDirect(toServer: string, message: Message) {
        this.connection.client.publish(message.topic.toString(), JSON.stringify({ fromServer: this.config.serverName, toServer, message }), () => { })
    }

    public send (message: Message) {
        this.connection.client.publish(message.topic.toString(), JSON.stringify({ fromServer: this.config.serverName, message }), () => { })
    }

    public subscribe<SpecificMessage> (topic: TOPIC | STATE_REGISTRY_TOPIC, callback: (message: SpecificMessage, originServerName: string) => void) {
        this.services.logger.debug(EVENT.INFO, `new subscription to topic ${TOPIC[topic] || STATE_REGISTRY_TOPIC[topic]}`)
        let callbacks = this.callbacks.get(topic)
        if (!callbacks) {
            callbacks = new Set()
            this.callbacks.set(topic, callbacks)
            this.subscribeConnection.client.subscribe(topic.toString())
        }
        callbacks.add(callback)
    }

    private onMessage (topic: TOPIC | STATE_REGISTRY_TOPIC, clusterMessage: string) {
        try {
            const { fromServer, toServer, message } = JSON.parse(clusterMessage)
            if (this.config.serverName === fromServer) {
                return
            }
            if (toServer !== undefined && this.config.serverName !== toServer) {
                return
            }
            const callbacks = this.callbacks.get(message.topic)
            if (!callbacks || callbacks.size === 0) {
                this.services.logger.error(EVENT.PLUGIN_ERROR, `Received message for unknown topic ${message.topic}`)
                return
            }
            callbacks.forEach((callback) => callback(message, fromServer))
        } catch (e) {
            this.services.logger.error(EVENT.PLUGIN_ERROR, `Error parsing message ${e.toString()}`)
            return
        }
    }
}
