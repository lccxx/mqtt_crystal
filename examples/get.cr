require "../src/mqtt_crystal"

MqttCrystal::Client.new(host: "172.17.0.1").get("pub/#") { |t, m| puts "#{t}, #{m}" }
