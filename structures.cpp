#include "structures.h"

bool IntList::is_empty(){
    return first == nullptr;
}

void IntList::push_back(int data){
    Node<int>* newElem = new Node(data);
    if (is_empty()){
        first = newElem;
        last = newElem;
        return;
    }
    last->next = newElem;
    last = newElem;
}

Node<int>* IntList::find(int index){
    int counter = 0;
    Node<int>* current = first;
    while (counter < index){
        current = current->next;
        ++counter;
    }
    return current;
}

void IntList::print(string delimiter){
    if (is_empty()) return;

    Node<int>* current = first;
    while(current){
        cout << current->data << delimiter;
        current = current->next;
    }
    cout << endl;
}

bool BoolList::is_empty(){
    return first == nullptr;
}

void BoolList::push_back(bool data){
    Node<bool>* newElem = new Node(data);
    if (is_empty()){
        first = newElem;
        last = newElem;
        return;
    }
    last->next = newElem;
    last = newElem;
}

bool StringList::is_empty(){
    return first == nullptr;
}

void StringList::push_back(string data){
    ++listSize;
    Node<string>* newElem = new Node(data);
    if (is_empty()){
        first = newElem;
        last = newElem;
        return;
    }
    last->next = newElem;
    last = newElem;
}

void StringList::print(string delimiter){
    if (is_empty()) return;

    Node<string>* current = first;
    while(current){
        cout << current->data << delimiter;
        current = current->next;
    }
    cout << endl;
}

Node<string>* StringList::find(int index){
    if (index >= listSize || index < 0) throw "Wrong index";

    int counter = 0;
    Node<string>* current = first;
    while (counter < index){
        current = current->next;
        ++counter;
    }
    return current;
}

Node<string>* StringList::word_find(const string& word){
    Node<string>* current = first;
    while(current->data != word){
        current = current->next;
        if (current->next == nullptr){break;}
    }
    return current;
}

string StringList::join(const char symbol){
    string joined;
    Node<string>* current = first;
    while (current){
        joined += current->data + symbol;
        current = current->next;
    }
    return joined;
}

int StringList::index_word(const string& word){
    Node<string>* current = first;
    int id = 0;
    while (current){
        if (current->data == word){return id;}
        ++id;
        current = current->next;
    }
    return -1;
}

bool StringMatrix::is_empty(){
    return firstCol == nullptr;
}

void StringMatrix::push_right(string text){
    MatrixNode* newElem = new MatrixNode(text);
    if (is_empty()){
        firstCol = newElem;
        lastCol = newElem;
        return;
    }
    lastCol->nextCol = newElem;
    lastCol = newElem;
}

void StringMatrix::push_down(string text, int colNum){
    MatrixNode* newElem = new MatrixNode(text);
    MatrixNode* currCol = firstCol;
    int cntr = 0;
    while (cntr != colNum){
        currCol = currCol->nextCol;
        ++cntr;
    }

    MatrixNode* currRow = currCol;
    while (currRow->nextRow != nullptr){
        currRow = currRow->nextRow;
    }
    currRow->nextRow = newElem;
}

void StringMatrix::print(){
    StringList out;
    for (auto col = firstCol; col != nullptr; col = col->nextCol){
        int currRow = 0;
        for (auto row = col; row != nullptr; row = row->nextRow){
            if (col == firstCol){
                out.push_back(row->data + (string(" ") * static_cast<int>(20 - (row->data).size())));
            }
            else{
                out.find(currRow)->data += row->data + (string(" ") * static_cast<int>(20 - (row->data).size()));
            }
            ++currRow;
        }
    }

    out.print("\n");
}

string operator*(const string& str, int n) {
    string result;
    for (int i = 0; i < n; ++i) {
        result += str;
    }
    return result;
}
