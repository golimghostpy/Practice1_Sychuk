#include "structures.h"

bool IntList::is_empty(){
    return first == nullptr;
}

void IntList::push_back(int data){ // добавление элемента в конец списка
    Node<int>* newElem = new Node(data);
    if (is_empty()){
        first = newElem;
        last = newElem;
        return;
    }
    last->next = newElem;
    last = newElem;
}

Node<int>* IntList::find(int index){ // нахождение элемента по индексу
    int counter = 0;
    Node<int>* current = first;
    while (counter < index){
        current = current->next;
        ++counter;
    }
    return current;
}

void IntList::print(string delimiter){ // вывод
    if (is_empty()) return;

    Node<int>* current = first;
    while(current){
        cout << current->data << delimiter;
        current = current->next;
    }
    cout << endl;
}

void IntList::clear(){ // очистка
    while (first != nullptr){
        Node<int>* temp = first;
        first = first->next;
        delete temp;
    }
    last = nullptr;
}

bool BoolList::is_empty(){
    return first == nullptr;
}

void BoolList::push_back(bool data){ // добавление эоемента в конец списка
    Node<bool>* newElem = new Node(data);
    if (is_empty()){
        first = newElem;
        last = newElem;
        return;
    }
    last->next = newElem;
    last = newElem;
}

void BoolList::clear(){ // очистка
    while (first != nullptr){
        Node<bool>* temp = first;
        first = first->next;
        delete temp;
    }
    last = nullptr;
}

bool StringList::is_empty(){
    return first == nullptr;
}

void StringList::push_back(string data){ // добавление элемента в конец списка
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

void StringList::print(string delimiter){ // вывод
    if (is_empty()) return;

    Node<string>* current = first;
    while(current){
        cout << current->data << delimiter;
        current = current->next;
    }
    cout << endl;
}

Node<string>* StringList::find(int index){ // нахождение элемента по индексу
    if (index >= listSize || index < 0) cout << index << endl;

    int counter = 0;
    Node<string>* current = first;
    while (counter < index){
        current = current->next;
        ++counter;
    }
    return current;
}

Node<string>* StringList::word_find(const string& word){ // нахождение элемента по значению
    Node<string>* current = first;
    while(current->data != word){
        current = current->next;
        if (current->next == nullptr){break;}
    }
    return current;
}

string StringList::join(const char symbol){ // соеденение списка в строку
    string joined;
    Node<string>* current = first;
    while (current){
        joined += current->data + symbol;
        current = current->next;
    }
    return joined;
}

int StringList::index_word(const string& word){ // получение индекса искомого слова
    Node<string>* current = first;
    int id = 0;
    while (current){
        if (current->data == word){return id;}
        ++id;
        current = current->next;
    }
    return -1;
}

void StringList::clear(){ // очистка
    while (first != nullptr){
        Node<string>* temp = first;
        first = first->next;
        delete temp;
    }
    last = nullptr;
}

bool StringMatrix::is_empty(){
    return firstCol == nullptr;
}

void StringMatrix::push_right(string text){ // добавление элемента в новый столбец
    MatrixNode* newElem = new MatrixNode(text);
    if (is_empty()){
        firstCol = newElem;
        lastCol = newElem;
        return;
    }
    lastCol->nextCol = newElem;
    lastCol = newElem;
}

void StringMatrix::push_down(string text, int colNum){ // добавление элемента в новую строку в определенный столбец
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

void StringMatrix::print(){ // вывод
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

void StringMatrix::clear(){ // очистка
    while (firstCol != nullptr){
        MatrixNode* tempCol = firstCol;
        firstCol = firstCol->nextCol;
        while (tempCol->nextRow != nullptr){
            MatrixNode* tempRow = tempCol->nextRow;
            tempCol->nextRow = tempRow->nextRow;
            delete tempRow;
        }
        delete tempCol;
    }
    lastCol = nullptr;
}

string operator*(const string& str, int n) { // переопределение операции умножения строки на число
    string result;
    for (int i = 0; i < n; ++i) {
        result += str;
    }
    return result;
}
