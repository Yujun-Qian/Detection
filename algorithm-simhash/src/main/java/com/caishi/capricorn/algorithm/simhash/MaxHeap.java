package com.caishi.capricorn.algorithm.simhash;

import java.util.*;

public class MaxHeap<E>
{
    private PriorityQueue<E> queue;

    private int maxSize;


    public MaxHeap(int maxSize, Comparator<E> comparator)
    {
        if (maxSize <= 0)
            throw new IllegalArgumentException();
        this.maxSize = maxSize;
        this.queue = new PriorityQueue<E>(maxSize, comparator);
    }


    public boolean add(E e)
    {
        if (queue.size() < maxSize)
        {
            queue.add(e);
            return true;
        }
        else
        {
            E peek = queue.peek();
            if (queue.comparator().compare(e, peek) > 0)
            {
                queue.poll();
                queue.add(e);
                return true;
            }
        }
        return false;
    }


    public MaxHeap<E> addAll(Collection<E> collection)
    {
        for (E e : collection)
        {
            add(e);
        }

        return this;
    }

    public List<E> toList()
    {
        ArrayList<E> list = new ArrayList<E>(queue.size());
        while (!queue.isEmpty())
        {
            list.add(0, queue.poll());
        }

        return list;
    }
}
