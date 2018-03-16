require "uuid"
require "../src/mqtt_crystal"

module Mqttdemo
  client = MqttCrystal::Client.new(id: "test1")
  p client.connect("172.17.0.1", 1883_u16, "liuchong", "xxxxxx")
  p client.subscribe("pub/#")
  spawn {
    while true
      packet = client.read_packet
      if packet.is_a?(MqttCrystal::Packet::Publish)
        packet = packet.as(MqttCrystal::Packet::Publish)
        puts "#{packet.topic}, #{packet.payload}"
      end
    end
  }
  999999.times { |i|
    client.publish("pub/oh-my", "test #{i}")
    sleep 0.01
  }
end
