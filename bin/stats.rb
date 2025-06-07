max_amount = -Float::INFINITY
min_amount = Float::INFINITY
line_count = 0
first_5 = []
last_5 = []
total_lines = `wc -l < data/output.csv`.to_i

puts total_lines

File.foreach("data/output.csv") do |line|
  amount = line.split(",")[3].to_f

  # Обновляем максимум/минимум
  max_amount = amount if amount > max_amount
  min_amount = amount if amount < min_amount

  # Сохраняем первые 5 значений
  first_5 << amount if line_count < 5

  # Обновляем последние 5 значений
  last_5.shift if last_5.size >= 5
  last_5 << amount

  line_count += 1
end

puts "Максимальное amount: #{max_amount}"
puts "Минимальное amount: #{min_amount}"
puts "Медианное значение: #{median}"
puts "Количество строк: #{line_count}"
puts "Первые 5 значений: #{first_5}"
puts "Последние 5 значений: #{last_5}"
