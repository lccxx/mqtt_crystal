require "../src/mqtt_crystal"

client = MqttCrystal::Client.new(host: "172.17.0.1")

99999.times { |i|
  client.publish("pub/test1", i.to_s)
  sleep rand.seconds
}
