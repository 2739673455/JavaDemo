package com.atguigu.array;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/*
MyArrayList模仿ArrayList的思路来实现动态数组。
MyArrayList是我们自己定义的一种集合类型。
<E>代表将来的元素类型
 */
public class MyArrayList<E> implements Iterable<E> {
    private Object[] elementData = new Object[5];
    private int size;//记录实际存储的元素的个数

    //增
    public void add(E e) {
        grow();//扩容
        elementData[size++] = e;
    }

    //选择需要抽取出来构造一个方法的代码，然后按快捷键Ctrl + Alt + M
    private void grow() {
        //判断是否是否已满，如果已满，需要先扩容
        if (size >= elementData.length) {
            //扩容1.5倍
            elementData = Arrays.copyOf(elementData, elementData.length + (elementData.length >> 1));
            //让elementData接收新数组的首地址，因为我们所有的方法中都是通过elementData变量来访问数组元素
        }
    }

    public void add(int index, E e) {
        //(1)先检查index的合法性[0,size]范围内是合法的
        if (index < 0 || index > size) {
            throw new IndexOutOfBoundsException(index + "位置越界了，正确的范围[0," + size + "]");
        }
        grow();//(2)扩容
//        (3)移动元素
        /*
        假设数组长度为5，size=4，index=1
        需要移动[1],[2],[3]，移动了size-index个
         */
        System.arraycopy(elementData, index, elementData, index + 1, size - index);
        //(4)将新元素放入[index]
        elementData[index] = e;
        //(5)元素个数增加
        size++;
    }

    //查询
    public E get(int index) {
        checkExistElementIndex(index);
        //(2)返回[index]位置的元素
        return (E) elementData[index];
    }

    //检查已有元素的下标范围
    private void checkExistElementIndex(int index) {
        //(1)检查已有元素的位置[0,size-1]
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException(index + "位置越界了，正确的范围[0," + (size - 1) + "]");
        }
    }

    public int indexOf(Object target) {
        //在[0,size-1]范围内查找target就可以了
        for (int i = 0; i < size; i++) {
            if (Objects.equals(elementData[i], target)) {
                return i;
            }
        }
        return -1;
    }

    public int lastIndexOf(Object target) {
        //在[0,size-1]范围内查找target就可以了
        for (int i = size - 1; i >= 0; i--) {
            if (Objects.equals(elementData[i], target)) {
                return i;
            }
        }
        return -1;
    }

    /*
    返回值是true，代表存在
     */
    public boolean contains(Object target) {
        //在[0,size-1]范围内查找target就可以了
       /* for(int i=0; i<size; i++){
            if(Objects.equals(elementData[i],target)){
                return true;
            }
        }
        return false;*/
       /* int index = indexOf(target);
        return index != -1;*/

        return indexOf(target) != -1;
    }

    public int size() {
        return size;
    }

    //删除
    public void remove(int index) {
        //(1)检查index合法性
        checkExistElementIndex(index);
        //(2)移动元素，左移
        /*
        假设数组的长度为5，元素个数size=4，index=1
        需要左移的元素有：[2],[3] 共移动size-index-1个
         */
        System.arraycopy(elementData, index + 1, elementData, index, size - index - 1);
        //(3)清理末尾位置的元素
        /*elementData[size-1] = null;
        size--;*/
        elementData[--size] = null;
    }

    public void remove(Object target) {
        //（1）调用indexOf方法来确定target的位置
        int index = indexOf(target);
        //（2）只要index!=-1,就说明target存在的
        if (index != -1) {
            remove(index);
        }
    }

    //替换
    public void set(int index, E e) {
        //(1)检查index合法性
        checkExistElementIndex(index);
        //(2)直接替换[index]位置的元素
        elementData[index] = e;
    }

    @Override
    public Iterator<E> iterator() {
        return new MyItr();
    }

    //定义一个内部类实现Iterator接口
    private class MyItr implements Iterator<E> {
        int cursor;//游标，迭代器当前指向的元素的下标
        //cursor的合理范围[0, size-1]

        @Override
        public boolean hasNext() {
            return cursor < size;
        }

        @Override
        public E next() {
            return (E) elementData[cursor++];
        }
    }
}