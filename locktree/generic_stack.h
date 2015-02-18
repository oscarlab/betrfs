/*
  Code from http://www.cprogramming.com/tutorial/computersciencetheory/stackcode.html
     -------------------------------------------------------------------
    |                                                                   |
    |    Stack Class                                                    |
    |    ===========================================================    |
    |    This Stack has been implemented with templates to allow it     |
    |    to accomodate virtually any data type, and the size of the     |
    |    Stack is determined dynamically at runtime.                    |
    |                                                                   |
    |    There is also a new function: peek(), which, given a whole     |
    |    number 'depth', returns the Stack element which is 'depth'     |
    |    levels from the top.                                           |
    |                                                                   |
     -------------------------------------------------------------------
*/

#ifndef __GenericStackClassH__
#define __GenericStackClassH__

#include <toku_assert.h>
//-------------------------------------------------
// Main structure of GenericStack Class:
//-------------------------------------------------

template <class Elem>
class GenericStack
{
public:
	GenericStack(int MaxSize=500);
	GenericStack(const GenericStack<Elem> &OtherStack);
	~GenericStack(void);
	inline bool        is_empty(void);
	inline void        push(const Elem &Item); // Adds Item to the top
	inline Elem        pop(void);              // Returns Item from the top
	inline const Elem &peek(int depth) const;  // Peek a depth downwards
	inline const Elem &top(void) const;

protected:
	Elem     *data;           // The actual data array
	int       curr_elem_num;    // The current number of elements
	const int MAX_NUM;        // Maximum number of elements
};

//-------------------------------------------------
// Implementation of GenericStack Class:
//-------------------------------------------------

// GenericStack Constructor function
template <class Elem>
GenericStack<Elem>::GenericStack(int MaxSize) :
	MAX_NUM(MaxSize)    // Initialize the constant
{
	data = (Elem *) toku_malloc(MAX_NUM * sizeof(*data));
	curr_elem_num = 0;
}

// GenericStack Destructor function
template <class Elem>
GenericStack<Elem>::~GenericStack(void)
{
	toku_free(data);
}

template <class Elem>
inline bool GenericStack<Elem>::is_empty(void)
{
	return curr_elem_num == 0;
}

// push() function
template <class Elem>
inline void GenericStack<Elem>::push(const Elem &Item)
{
	// Error Check: Make sure we aren't exceeding the maximum storage space
	assert(curr_elem_num < MAX_NUM);
  
	data[curr_elem_num++] = Item;
}

// pop() function
template <class Elem>
inline Elem GenericStack<Elem>::pop(void)
{
	// Error Check: Make sure we aren't popping from an empty GenericStack
	assert(curr_elem_num > 0);

	return data[--curr_elem_num];
}

template <class Elem>
inline const Elem &GenericStack<Elem>::top(void) const
{
	// Error Check: Make sure we aren't popping from an empty GenericStack
	assert(curr_elem_num > 0);

	return data[curr_elem_num-1];
}


// peek() function
template <class Elem>
inline const Elem &GenericStack<Elem>::peek(int depth) const
{
	// Error Check: Make sure the depth doesn't exceed the number of elements
	assert(depth < curr_elem_num);

	return data[curr_elem_num - (depth + 1)];
}

#endif /*__GenericStackClassH__*/
