#!/usr/bin/env ruby

require "mqtt"; require "digest"; get_count, err_count = 0, 0, 0; Thread.new { loop { sleep 1; puts "#{[ get_count, err_count ]}" } }; MQTT::Client.connect("172.17.0.1").get("lccc/verify/test/#") { |t, m| get_count += 1; err_count += 1 if Digest::MD5.hexdigest(t) != m }
