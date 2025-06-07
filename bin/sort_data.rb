#!/usr/bin/env ruby
require_relative "../lib/data_sorter"

if ARGV.size < 2
  puts "Usage: ruby sort_data.rb <input_file> <output_file> [options]"
  puts "Options:"
  puts "  --log-level=LEVEL       Set log level (default: info)"
  puts "  --max-open-files=NUM    Set maximum open files (default: 255)"
  puts "  --lines-chunk-size=NUM  Set lines read limit (default: 200000)"
  puts "  --buffer-size=BYTES     Set buffer size in bytes (default: 2097152)"
  exit 1
end

input_path, output_path = ARGV[0], ARGV[1]
settings = {}

# Parse optional arguments
ARGV[2..].each do |arg|
  case arg
  when /--log-level=(.+)/
    settings[:log_level] = $1.to_sym
  when /--max-open-files=(\d+)/
    settings[:max_open_files] = $1.to_i
  when /---lines-chunk-size=(\d+)/
    settings[:lines_chunk_size] = $1.to_i
  when /--buffer-size=(\d+)/
    settings[:buffer_size] = $1.to_i
  else
    puts "Unknown argument: #{arg}"
    exit 1
  end
end

unless File.exist?(input_path)
  puts "Input file not found: #{input_path}"
  exit 1
end

puts "Sorting file: #{input_path}"
DataSorter.new(input_path, output_path, settings).run
puts "Done. Output saved to: #{output_path}"
