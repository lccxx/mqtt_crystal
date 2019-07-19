require "../src/mqtt_crystal"

t, m = MqttCrystal::Client.new(host: "172.17.0.1").subscribe("pub/#").receive
puts "#{t}, #{m}"
