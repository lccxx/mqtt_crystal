require "spec"
require "uuid"
require "../src/mqtt_crystal"

def slice_it(a : String) : Bytes
  bytes = a.bytes
  slice = Bytes.new bytes.size
  bytes.map_with_index { |b, i| slice[i] = b }
  slice
end
