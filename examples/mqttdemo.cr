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
      pp client.channel.receive
    end
  }

  client.connect.subscribe("pub/#")

  9999.times { sleep 9999 }
end
