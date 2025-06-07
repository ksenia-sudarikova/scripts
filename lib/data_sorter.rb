# frozen_string_literal: true

require "tempfile"
require_relative "../lib/max_heap"
require_relative "../lib/merge_sort"

class Transaction
  attr_reader :timestamp, :transaction_id, :user_id, :amount

  def initialize(timestamp, transaction_id, user_id, amount)
    @timestamp = timestamp
    @transaction_id = transaction_id
    @user_id = user_id
    @amount = amount.to_f
  end

  def to_line
    "#{timestamp},#{transaction_id},#{user_id},#{format("%.2f", amount)}\n"
  end
end

class DataSorter
  PID = Process.pid
  CHUNK_LIST_FILE_NAME = "tmp/chunks/list.txt"

  def parse_line(line)
    line.strip!
    parts = line.split(",", 5) # Limit to 5 parts to catch extra data
    raise StandardError, "Unknown Data in line #{line}" if parts.size > 4
    raise StandardError, "Unknown Data in #{line}" if parts.size < 4

    Transaction.new(parts[0], parts[1], parts[2], parts[3])
  end

  def initialize(input_path, output_path, settings = {})
    @input_path = input_path
    @output_path = output_path
    @lines_chunk_size = settings.fetch(:lines_chunk_size, 200_000)
    @max_open_files = settings.fetch(:max_open_files, 255)
    @buffer_size = settings.fetch(:buffer_size, 2_097_152)
    @log_level = settings.fetch(:log_level, :info)
  end

  def run
    merge_sort_lines_in_chunks
    merge_files
    check_sorted
  end

  def merge_sort_lines_in_chunks
    reset_files
    chunk_count = 0

    File.open(@input_path, "r") do |file|
      loop do
        lines = Array.new(@lines_chunk_size)
        i = 0
        while i < @lines_chunk_size && (line = file.gets)
          lines[i] = line
          i += 1
        end
        break if i == 0

        transactions = lines[0...i].map! { |line| [parse_line(line).amount, line] }
        # not allowed to use ruby sort, x2 faster then merge_sort, x4 faster then binary heap
        # sorted_lines = transactions.sort_by { |amount, _| -amount }
        sorted_lines = MergeSort.run(transactions)

        write_chunk(sorted_lines, chunk_count)

        chunk_count += 1
        # break if chunk_count >= 30
      end
    end
  end

  def write_chunk(lines, chunk_count)
    file_path = "tmp/chunks/chunk_#{chunk_count}.txt"

    File.open(file_path, "w") do |f|
      f.puts lines.map { |_, original_line| original_line }
    end

    print_process_memory("Chunk #{chunk_count} processed merge_sort,") if (chunk_count % 10).zero?

    File.open(CHUNK_LIST_FILE_NAME, "a") { |f| f.puts(file_path) }
  end

  def cleanup
    ObjectSpace.each_object(Tempfile).each(&:close!)
  end

  # It's a bit over engineering to merge files in groups,
  # but I had problem on my OS with to many opened files, when CHUNK_SIZE were lower
  # and this code actually can be useful for files bigger then 2Gb
  def merge_files
    input_files = File.readlines(CHUNK_LIST_FILE_NAME, chomp: true)

    # merge groups of files that fit within MAX_OPEN_FILES limit
    intermediate_files = []
    input_files.each_slice(@max_open_files) do |file_group|
      intermediate_files << merge_file_group(file_group)
    end

    # if we have more than one intermediate file, merge them recursively
    if intermediate_files.size > 1
      merge_files(intermediate_files, @output_path)
    else
      # final merge is just renaming the single intermediate file
      File.rename(intermediate_files.first, @output_path)
    end
  end

  def merge_file_group(files)
    # read first lines from each file in group
    readers = files.map do |path|
      file = File.open(path, "rb")
      [file, file.gets]
    end.reject { |(_, line)| line.nil? }

    output_temp = Tempfile.new("merge_#{PID}", binmode: true)
    # temporary save sorted lines in buffer
    # when buffer is bigger than capacity write it to file
    output_buffer = String.new(capacity: @buffer_size)

    begin
      # load first lined to heap
      heap = MaxHeap.new
      readers.each do |(file, line)|
        # fast parsing without object
        amount = line.rpartition(",").last.to_f
        heap.add(amount, {file: file, line: line})
      end

      until heap.empty?
        # put first line from heap to buffer
        entry = heap.pop
        output_buffer << entry[:line]

        # read next line, and put it to heap
        if (next_line = entry[:file].gets)
          next_amount = next_line.rpartition(",").last.to_f
          heap.add(next_amount, {file: entry[:file], line: next_line})
        end

        # flush buffer to file in @buffer_size chunks
        if output_buffer.bytesize >= @buffer_size
          output_temp.write(output_buffer)
          output_buffer.clear
          print_process_memory "#{entry[:line]} buffer_was_cleared"
        end
      end

      output_temp.write(output_buffer) unless output_buffer.empty?
    ensure
      readers.each { |(file, _)| file.close }
      output_temp.close
    end

    output_temp.path
  end

  def reset_files
    Dir.mkdir("tmp") unless Dir.exist?("tmp")
    Dir.glob("tmp/chunks/*").each { |f| File.delete(f) if File.file?(f) }
    Dir.rmdir("tmp/chunks") if Dir.exist?("tmp/chunks")
    Dir.mkdir("tmp/chunks") unless Dir.exist?("tmp/chunks")
  end

  def print_process_memory(message)
    return unless @log_level == :debug

    counts = ObjectSpace.count_objects
    memory_kb = `ps -o rss= -p #{PID}`.to_i
    puts "#{message} - Memory: #{memory_kb / 1024.0} MB | " \
         "Strings: #{counts[:T_STRING]} | " \
         "Arrays: #{counts[:T_ARRAY]} | " \
         "Objects: #{counts[:T_OBJECT]}"
  end

  def check_sorted
    return unless @log_level == :debug

    prev_amount = Float::INFINITY
    File.foreach(@output_path).with_index do |line, line_num|
      current_amount = line.split(",")[3].to_f
      if current_amount > prev_amount
        puts "Предыдущее amount: #{prev_amount}, текущее: #{current_amount}"
        raise "Ошибка сортировки на строке #{line_num + 1}:"
      end
      prev_amount = current_amount
    end
    true
  end
end
