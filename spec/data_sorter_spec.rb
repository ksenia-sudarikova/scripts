require "spec_helper"
require_relative "../lib/data_sorter"

describe DataSorter do
  let(:input) { "spec/fixtures/data_small.csv" }
  let(:output) { "tmp/output.txt" }
  let(:data_sorter) { DataSorter.new(input, output) }
  let(:good_data) do
    {
      timestamp: "2023-09-03T12:45:00Z",
      transaction_id: "txn12345",
      user_id: "user987",
      amount: "500.25"
    }
  end

  describe "#parse_line" do
    context "normal line" do
      it "parses line correctly" do
        line = "2023-09-03T12:45:00Z,txn12345,user987,500.25"
        txn = data_sorter.parse_line(line)
        expect(txn.transaction_id).to eq("txn12345")
        expect(txn.amount).to eq(500.25)
      end

      it "return transaction with good data" do
        line = good_data.map { |k, v| v }.join(",")

        txn = data_sorter.parse_line(line)
        expect(txn.timestamp).to eq(good_data[:timestamp])
        expect(txn.transaction_id).to eq(good_data[:transaction_id])
        expect(txn.user_id).to eq(good_data[:user_id])
        expect(txn.amount).to eq(good_data[:amount].to_f)
      end
    end

    context "wrong lines:" do
      it "raises error one field raises error" do
        line = good_data.map { |k, v| v }.join("")

        expect { data_sorter.parse_line(line) }.to raise_error(Exception)
      end

      it "raises error bad amount" do
        line = good_data.map do |k, v|
          if k == :amount
            "notnumber"
          else
            v
          end
        end

        expect { data_sorter.parse_line(line) }.to raise_error(Exception)
      end

      it "raises error when one more field" do
        line = good_data.map do |k, v|
          if k == :user_id
            "v,add_comma_to_user"
          else
            v
          end
        end.join(",")

        expect { data_sorter.parse_line(line) }.to raise_error(Exception)
      end
    end
  end

  describe "#merge_sort_lines_in_chunks boundaries" do
    let(:output) { "tmp/output.txt" }

    def create_input_file(lines)
      path = "tmp/test_input.csv"
      File.write(path, lines.map.with_index { |amt, i|
        "2023-09-03T12:00:00Z,txn#{i},user#{i},#{amt}"
      }.join("\n"))
      path
    end

    def chunk_files
      Dir.glob("tmp/chunks/chunk_*.txt")
    end

    before do
      Dir.mkdir("tmp") unless Dir.exist?("tmp")
    end

    after do
      FileUtils.rm_rf("tmp")
    end

    it "creates 1 chunk when input lines == lines_chunk_size" do
      input = create_input_file(Array.new(5, 100.0))
      sorter = DataSorter.new(input, output, lines_chunk_size: 5)
      sorter.send(:merge_sort_lines_in_chunks)

      expect(chunk_files.size).to eq(1)
    end

    it "creates 2 chunks when input lines == lines_chunk_size + 1" do
      input = create_input_file(Array.new(6, 100.0))
      sorter = DataSorter.new(input, output, lines_chunk_size: 5)
      sorter.send(:merge_sort_lines_in_chunks)

      expect(chunk_files.size).to eq(2)
    end

    it "creates 1 chunk when input has only 1 line" do
      input = create_input_file([100.0])
      sorter = DataSorter.new(input, output, lines_chunk_size: 5)
      sorter.send(:merge_sort_lines_in_chunks)

      expect(chunk_files.size).to eq(1)
    end
  end

  describe "#run" do
    before do
      data_sorter.run
    end

    context "small chunk_size" do
      let(:data_sorter) { DataSorter.new(input, output, {lines_chunk_size: 2}) }

      it "return max on top" do
        lines = File.readlines(output)
        expect(lines.first).to include("503.00")
      end

      it "return min on bottom" do
        lines = File.readlines(output)
        expect(lines.last).to include("60.00")
      end

      it "does external sort correctly" do
        lines = File.readlines(output)
        sorted = lines.map { |line| data_sorter.parse_line(line) }.map { |h| h.amount }
        expect(sorted).to eq([503.0, 401.1, 401.0, 100.0, 99.0, 95.0, 90.0, 80.0, 60.0])
      end
    end

    context "default chunk_size" do
      it "return max on top" do
        lines = File.readlines(output)
        expect(lines.first).to end_with("503.00\n")
      end

      it "return min on bottom" do
        lines = File.readlines(output)
        expect(lines.last).to end_with("60.00\n")
      end

      it "does external sort correctly" do
        File.readlines(input).sort { |l| l.split(",", 4)[3].to_f }
        lines = File.readlines(output)
        sorted = lines.map { |line| data_sorter.parse_line(line) }.map { |h| h.amount }
        expect(sorted).to eq([503.00, 401.10, 401.00, 100.00, 99.00, 95.00, 90.00, 80.00, 60.00])
      end
    end
  end
end
