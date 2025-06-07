class MergeSort
  # array of entries, where each entry array
  # first value is amount for compare
  # second data that need to be sorted
  def self.run(array)
    merge_sort_recursive(array)
  end

  def self.merge_sort_recursive(array)
    return array if array.size <= 1

    mid = array.size / 2
    left_sorted = merge_sort_recursive(array[0...mid])
    right_sorted = merge_sort_recursive(array[mid..])
    merge_sorted_subarrays(left_sorted, right_sorted)
  end

  def self.merge_sorted_subarrays(left_array, right_array)
    merged_array = []
    left_index = 0
    right_index = 0

    # merge two sorted arrays into one by comparing transaction amounts
    while left_index < left_array.size && right_index < right_array.size
      # l_amount = left_array[left_index][0]
      # r_amount = right_array[right_index][0]
      if left_array[left_index][0] >= right_array[right_index][0]
        merged_array << left_array[left_index]
        left_index += 1
      else
        merged_array << right_array[right_index]
        right_index += 1
      end
    end

    # append any remaining elements
    merged_array.concat(left_array[left_index..]) if left_index < left_array.size
    merged_array.concat(right_array[right_index..]) if right_index < right_array.size

    merged_array
  end
end
