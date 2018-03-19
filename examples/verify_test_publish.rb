#!/usr/bin/env ruby

require "mqtt"; require "digest"; pub_count = 0; Thread.new { loop { sleep 1; puts "#{[ pub_count ]}" } }; client = MQTT::Client.connect("172.17.0.1"); 10000.times { sleep rand / 10; topic = "lccc/verify/test/#{rand}"; client.publish(topic, Digest::MD5.hexdigest(topic), false, 1); pub_count += 1 }
