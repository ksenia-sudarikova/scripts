#!/usr/bin/env ruby
require "time"

# OUTPUT_FILE = "data/test_data_large.csv"
OUTPUT_FILE = "data/test_data_medium.csv"
TARGET_SIZE_BYTES = (1024 * 1024 * 1024) / 2 # 500mb
# TARGET_SIZE_BYTES = 2 * 1024 * 1024 * 1024 # 2GB
BATCH_SIZE = 500_000

def fast_line(i)
  timestamp = Time.at(1_600_000_000 + i % 10_000).utc.iso8601
  txn_id = "txn#{i}"
  user_id = "user#{i % 10_000}"
  amount = "#{rand(0..9999)}.#{rand(0..99).to_s.rjust(2, "0")}"
  "#{timestamp},#{txn_id},#{user_id},#{amount}\n"
end

File.open(OUTPUT_FILE, "w") do |file|
  total_bytes = 0

  while total_bytes < TARGET_SIZE_BYTES
    lines = Array.new(BATCH_SIZE) { |i| fast_line(i) }
    buffer = lines.join
    file.write(buffer)
    total_bytes += buffer.bytesize

    puts "Progress: #{(total_bytes / 1024.0 / 1024.0).round(2)} MB"
  end
end

puts "✅ Done. File size: #{(File.size(OUTPUT_FILE) / 1024.0 / 1024.0).round(2)} MB"
