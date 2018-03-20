require "openssl"
require "../src/mqtt_crystal"

pub_count = 0

stop = false

Signal::INT.trap {
  stop = true
  sleep 3
  Process.exit 0
}

spawn {
  MqttCrystal::Client.new(host: "iot.eclipse.org").connect { |client|
    1000.times {
      break if stop
      topic = "lccc/verify/test/#{rand}"
      client.publish(topic, OpenSSL::MD5.hash(topic).map { |c| "%02x" % c }.join)
      pub_count += 1
      sleep (rand * 99).milliseconds
    }
  }
}

loop do
  sleep 1
  puts "pub_count: #{pub_count}"
end
