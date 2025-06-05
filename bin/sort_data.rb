#!/usr/bin/env ruby
require_relative "../lib/data_sorter"

if ARGV.size != 2
  puts "Usage: ruby sort_data.rb <input_file> <output_file>"
  exit 1
end

input_path, output_path = ARGV

unless File.exist?(input_path)
  puts "❌ Input file not found: #{input_path}"
  exit 1
end

puts "📦 Sorting file: #{input_path}"
DataSorter.sort_large_file(input_path, output_path)
puts "✅ Done! Output saved to: #{output_path}"
