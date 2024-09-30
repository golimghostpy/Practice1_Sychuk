#ifndef STRUCTURES_H_INCLUDED
#define STRUCTURES_H_INCLUDED

#include "libs.h"

template <typename T>
struct Node{
    T data;
    Node* next;
    Node(T val) : data(val), next(nullptr) {}
};

struct IntList{
    Node<int>* first;
    Node<int>* last;

    IntList() : first(nullptr), last(nullptr) {}

    bool is_empty();
    void push_back(int);
    Node<int>* find(int);
    void print(string);
};

struct BoolList{
    Node<bool>* first;
    Node<bool>* last;

    BoolList() : first(nullptr), last(nullptr) {}

    bool is_empty();
    void push_back(bool);
};

struct StringList{
    int listSize;
    Node<string>* first;
    Node<string>* last;

    StringList() : listSize(0), first(nullptr), last(nullptr) {}

    bool is_empty();
    void push_back(string);
    void print(string);
    Node<string>* find(int);
    Node<string>* word_find(const string&);
    string join(const char);
    int index_word(const string&);
};

struct MatrixNode{
    string data;
    MatrixNode* nextRow;
    MatrixNode* nextCol;

    MatrixNode(string val) : data(val), nextRow(nullptr), nextCol(nullptr) {}
};

struct StringMatrix{
    MatrixNode* firstCol;
    MatrixNode* lastCol;

    StringMatrix() : firstCol(nullptr), lastCol(nullptr){}

    bool is_empty();
    void push_right(string);
    void push_down(string, int);
    void print();
};

string operator*(const string&, int);

#endif // STRUCTURES_H_INCLUDED
