require "uuid"
require "../src/mqtt_crystal"

module Mqttdemo
  client = MqttCrystal::Client.new(id: UUID.random.to_s,
                                   host: "172.17.0.1",
                                   port: 1883_u16,
                                   username: "liuchong",
                                   password: "xxxxxx")

  spawn {
    loop do
      pp packet = client.channel.receive
      if packet.is_a?(MqttCrystal::Packet::Publish)
        packet = packet.as(MqttCrystal::Packet::Publish)
        client.socket.write MqttCrystal::Packet::Puback.new(id: packet.id).bytes if packet.qos > 0
      end
    end
  }

  client.connect.subscribe("lccc/receive/#")

  9999.times {
    sleep 15
    client.publish "lccc/receive/test", rand.to_s
  }
end
