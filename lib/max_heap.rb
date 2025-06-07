class MaxHeap
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

  private

  def bubble_up(index)
    parent = (index - 1) / 2
    return if index <= 0 || @heap[parent].priority > @heap[index].priority
    swap(index, parent)
    bubble_up(parent)
  end

  def bubble_down(index)
    left = 2 * index + 1
    right = 2 * index + 2
    selected = index

    if left < @heap.size && @heap[left].priority > @heap[selected].priority
      selected = left
    end
    if right < @heap.size && @heap[right].priority > @heap[selected].priority
      selected = right
    end
    return if selected == index

    swap(index, selected)
    bubble_down(selected)
  end

  def swap(i, j)
    @heap[i], @heap[j] = @heap[j], @heap[i]
  end
end
