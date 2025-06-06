# frozen_string_literal: true
require "tempfile"
require "securerandom"

class Transaction
  attr_reader :timestamp, :transaction_id, :user_id, :amount

  def initialize(timestamp, transaction_id, user_id, amount)
    @timestamp = timestamp
    @transaction_id = transaction_id
    @user_id = user_id
    @amount = amount.to_f
  end

  def to_line
    "#{timestamp},#{transaction_id},#{user_id},#{format('%.2f', amount)}\n"
  end
end

class DataSorter
  CHUNK_SIZE = 200_000
  PID = Process.pid
  MAX_OPEN_FILES = 255
  CHUNK_LIST_FILE_NAME = "tmp/chunks/list.txt"
  BUFFER_SIZE = 2_097_152

  def self.parse_line(line)
    line.strip!
    parts = line.split(",", 5) # Limit to 5 parts to catch extra data
    raise StandardError, "Unknown Data in line #{line}" if parts.size > 4

    Transaction.new(parts[0], parts[1], parts[2], parts[3])
  end

  def self.sort_large_file(input_path, output_path, chunk_size = CHUNK_SIZE)
    sort_lines_in_chunks(input_path, chunk_size)

    merge_files(output_path)
    check_sorted(output_path)
  end

  def self.sort_lines_in_chunks(input_path, chunk_size)
    reset_files
    File.truncate(CHUNK_LIST_FILE_NAME, 0) if File.exist?(CHUNK_LIST_FILE_NAME)

    heap = BinaryMaxHeap.new
    chunk_count = 0

    File.open(input_path, "r") do |file|
      file.each_line.with_index do |line, index|
        transaction = parse_line(line)
        heap.add(transaction.amount, transaction)

        # When chunk is full or EOF
        if (index + 1) % chunk_size == 0 || file.eof?
          write_chunk(heap)
          chunk_count += 1
          heap = BinaryMaxHeap.new # Reset heap for next chunk

          if chunk_count % 3 == 0
            cleanup
            print_process_memory("Chunk #{chunk_count} processed,")
          end
        end
      end
    end
  end

  def self.write_chunk(heap)
    file_path = "tmp/chunks/chunk#{SecureRandom.hex}.txt"

    File.open(file_path, "w") do |f|
      until heap.empty?
        f.write(heap.pop.to_line)
      end
    end

    File.open(CHUNK_LIST_FILE_NAME, "a") { |f| f.puts(file_path) }
  end

  def self.cleanup
    GC.start
    ObjectSpace.each_object(Tempfile).each(&:close!)
  end

  # It's a bit overengeniring to merge files in groups,
  # but I had problem on my OS with to many opened files, when CHUNK_SIZE were lower
  # and this code actually can be useful for files bigger then 2Gb
  def self.merge_files(output_file, max_open_files = MAX_OPEN_FILES)
    input_files = File.readlines(CHUNK_LIST_FILE_NAME, chomp: true)

    # merge groups of files that fit within MAX_OPEN_FILES limit
    intermediate_files = []
    input_files.each_slice(max_open_files) do |file_group|
      intermediate_files << merge_file_group(file_group)
      print_process_memory("create intermediate_files,")
    end

    # if we have more than one intermediate file, merge them recursively
    if intermediate_files.size > 1
      print_process_memory("merge intermediate_files,")
      merge_files(intermediate_files, output_file)
    else
      # final merge is just renaming the single intermediate file
      File.rename(intermediate_files.first, output_file)
    end
  end

  def self.merge_file_group(files)
    # read first lines from each file in group
    readers = files.map do |path|
      file = File.open(path, "rb")
      [file, file.gets]
    end.reject { |(_, line)| line.nil? }

    output_temp = Tempfile.new("merge_#{PID}", binmode: true)
    # temporary save sorted lines in buffer
    # when buffer is bigger than capacity write it to file
    output_buffer = String.new(capacity: BUFFER_SIZE)

    begin
      # load first lined to heap
      heap = BinaryMaxHeap.new
      readers.each do |(file, line)|
        # fast parsing without object
        amount = line.split(',', 4)[3].to_f
        heap.add(amount, { file: file, line: line })
      end

      until heap.empty?
        # put first line from heap to buffer
        entry = heap.pop
        output_buffer << entry[:line]

        # read next line, and put it to heap
        if next_line = entry[:file].gets
          next_amount = next_line.split(',', 4)[3].to_f
          heap.add(next_amount, { file: entry[:file], line: next_line })
        end

        # flush buffer to file in BUFFER_SIZE chunks
        if output_buffer.bytesize >= BUFFER_SIZE
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

  def self.reset_files
    Dir.glob("tmp/chunks/*").each { |f| File.delete(f) if File.file?(f) }
    Dir.rmdir("tmp/chunks") if Dir.exist?("tmp/chunks")
    Dir.mkdir("tmp") unless Dir.exist?("tmp")
    Dir.mkdir("tmp/chunks") unless Dir.exist?("tmp/chunks")
  end

  def self.print_process_memory(message)
    counts = ObjectSpace.count_objects
    memory_kb = `ps -o rss= -p #{PID}`.to_i
    puts "#{message} - Memory: #{memory_kb / 1024.0} MB | " \
         "Strings: #{counts[:T_STRING]} | " \
         "Arrays: #{counts[:T_ARRAY]} | " \
         "Objects: #{counts[:T_OBJECT]}"
  end

  def self.check_sorted(file_path)
    prev_amount = Float::INFINITY
    File.foreach(file_path).with_index do |line, line_num|
      current_amount = line.split(",")[3].to_f
      if current_amount > prev_amount
        puts "Ошибка сортировки на строке #{line_num + 1}:"
        puts "Предыдущее amount: #{prev_amount}, текущее: #{current_amount}"
        return false
      end
      prev_amount = current_amount
    end
    puts "Файл корректно отсортирован"
    true
  end
end


# binary heap for merging
class BinaryMaxHeap
  Entry = Struct.new(:priority, :value)

  def initialize
    @heap = []
  end

  def add(priority, value)
    @heap << Entry.new(priority, value)
    bubble_up(@heap.size - 1)
  end

  def pop
    return nil if @heap.empty?
    swap(0, @heap.size - 1)
    item = @heap.pop
    bubble_down(0) unless @heap.empty?

    item.value
  end

  def empty?
    @heap.empty?
  end

  def length
    @heap.length
  end

  private

  def bubble_up(index)
    parent = (index - 1) / 2
    return if index <= 0 || @heap[parent].priority >= @heap[index].priority
    swap(index, parent)
    bubble_up(parent)
  end

  def bubble_down(index)
    left = 2 * index + 1
    right = 2 * index + 2
    largest = index

    largest = left if left < @heap.size && @heap[left].priority > @heap[largest].priority
    largest = right if right < @heap.size && @heap[right].priority > @heap[largest].priority
    return if largest == index

    swap(index, largest)
    bubble_down(largest)
  end

  def swap(i, j)
    @heap[i], @heap[j] = @heap[j], @heap[i]
  end
end