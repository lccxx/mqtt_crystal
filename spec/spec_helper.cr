require "spec"
require "uuid"
require "../src/mqtt_crystal"

def slice_it(a : String) : Bytes
  bytes = a.bytes
  slice = Bytes.new bytes.size
  bytes.map_with_index { |b, i| slice[i] = b }
  slice
end

def with_mosquitto
  config = File.tempfile("mosquitto.conf")
  config << "port 1883"
  config << "allow_anonymous true"

  ready = Channel(Bool).new
  done = Channel(Bool).new
  spawn do
    Process.run("mosquitto", ["-c", config.path], input: Process::Redirect::Close) { |p|
      loop {
        break if p.error.read_line.includes? "on port 1883"
      }
      ready.send true
      # Cleanup mosquitto after test finishes
      done.receive
      begin
        p.kill
      rescue
      end
    }
  end

  ready.receive
  begin
    yield
  ensure
    done.send true
    config.delete
  end
end
