#include "libs.h"
#include "structures.h"

int tuplesLim;

StringList take_section(StringList& source, unsigned int frontInd, unsigned int backInd){
    StringList out;
    unsigned int currInd = frontInd;
    while (currInd != backInd){
        out.push_back(source.find(currInd)->data);
        ++currInd;
    }
    return out;
}

void check_active(const string& genPath, const StringList& tables){
    cout << "Now all tables will be checking until they all are free" << endl;
    while (true){
        bool isFree = true;
        for (auto i = tables.first; i != nullptr; i = i->next){
            string path = genPath + i->data + "/" + i->data + "_lock.txt";
            ifstream checkActive(path);
            string line;
            checkActive >> line;
            checkActive.close();
            if (line == "1"){
                isFree = false;
            }
        }

        if (isFree) {break;}
    }
    cout << "Now your reques will be completed" << endl;
}

void make_active(const string& genPath, const StringList& tables){
    for (auto i = tables.first; i != nullptr; i = i->next){
        string path = genPath + i->data + "/" + i->data + "_lock.txt";
        ofstream makeActive(path);
        makeActive << 1;
        makeActive.close();
    }
}

void make_inactive(const string& genPath, const StringList& tables){
    for (auto i = tables.first; i != nullptr; i = i->next){
        string path = genPath + i->data + "/" + i->data + "_lock.txt";
        ofstream makeInactive(path);
        makeInactive << 0;
        makeInactive.close();
    }
}

string create_db(){
    ifstream inFile("schema.json");
    nlohmann::json schema;
    inFile >> schema;

    tuplesLim = (int)schema["tuples_limit"];
    string name = schema["name"];
    if (filesystem::is_directory(name)){
        return (string)name;
    }
    filesystem::path basePath = name;

    for (auto i : schema["structure"].items()){
        filesystem::path dirPath = basePath / i.key();
        filesystem::create_directories(dirPath);

        ofstream outfile(dirPath / "1.csv");
        outfile << (i.key() + "_pk;");
        for (auto column = 0; column < i.value().size(); ++column){
            if (column == i.value().size() - 1){
                outfile << (string)i.value()[column] << endl;
            }else{
                outfile << (string)i.value()[column] << ";";
            }
        }
        outfile.close();

        ofstream lockfile(dirPath / (i.key() + "_lock.txt"));
        lockfile << "0";
        lockfile.close();

        ofstream keys(dirPath / (i.key() + "_pk_sequence.txt"));
        keys << "1";
        keys.close();
    }
    return (string)name;
}

enum class SQLRequest{
    SELECT,
    INSERT,
    DELETE,
    END,
    UNKNOWN
};

SQLRequest get_com (const string& command){
    if (command == "SELECT") {return SQLRequest::SELECT;}
    if (command == "INSERT") {return SQLRequest::INSERT;}
    if (command == "DELETE") {return SQLRequest::DELETE;}
    if (command == "end") {return SQLRequest::END;}
    return SQLRequest::UNKNOWN;
}

StringList split(const string& str, const string& delimiter) {
    StringList result;
    string currentPart;
    int delimiterLength = delimiter.size();

    for (auto i = 0; i < str.size(); ++i) {
        int j = 0;
        while (j < delimiterLength && i + j < str.size() && str[i + j] == delimiter[j]) {
            ++j;
        }

        if (j == delimiterLength) {
            if (currentPart != "") {
                result.push_back(currentPart);
                currentPart = "";
            }
            i += delimiterLength - 1;
        } else {
            currentPart += str[i];
        }
    }

    if (!currentPart.empty()) {
        result.push_back(currentPart);
    }

    return result;
}

string remove_extra(string& removeFrom){
    string newStr;
    for (auto i: removeFrom){
        if (i == '(' || i == '\'' || i == ')' || i == ',' || i == ' '){
            continue;
        }
        else {newStr += i;}
    }
    return newStr;
}

void write_in_csv(const string& path, StringList text){
    ofstream out(path, ios_base::app);
    for (int i = 0; i < text.listSize; ++i){
        if(i != text.listSize - 1){
            out << text.find(i)->data << ";";
        }
        else {
            out << text.find(i)->data << endl;
        }
    }
}

void insert_into(const string& schemaName, StringList command){
    string table = command.find(2)->data;

    StringList tables;
    tables.push_back(table);
    check_active(schemaName + "/", tables);
    make_active(schemaName + "/", tables);

    ifstream headerRead(schemaName + '/' + table + '/' + "1.csv");
    string header;
    headerRead >> header;
    headerRead.close();

    StringList data;
    ifstream pkRead(schemaName + '/' + table + '/' + table + "_pk_sequence.txt");
    string idStr;
    getline(pkRead, idStr);
    pkRead.close();
    data.push_back(idStr);
    int newID = stoi(idStr) + 1;

    ofstream pkWrite(schemaName + '/' + table + '/' + table + "_pk_sequence.txt");
    pkWrite << to_string(newID);
    pkWrite.close();

    for (int i = 4; i < command.listSize; ++i){
        data.push_back(remove_extra(command.find(i)->data));
    }

    string path;
    int currFile = 1;
    do{
        path = schemaName + '/' + table + '/';
        ifstream check(path + to_string(currFile) + ".csv");
        if (check.bad()){
            break;
        }
        int cntLines = 0;
        string line;
        while(check >> line){
            ++cntLines;
        }

        if (cntLines <= tuplesLim){
            path += to_string(currFile) + ".csv";

            if (currFile > 1){
                ofstream headerWrite(path);
                headerWrite << header << endl;
                headerWrite.close();
            }

            write_in_csv(path, data);
            break;
        }
        ++currFile;
    }while(true);

    make_inactive(schemaName + "/", tables);
}

bool check_filter_delete(StringList& header, StringList& text, const string& filter){
    StringList orSplited = split(filter, " or ");
    for (Node<string>* i = orSplited.first; i != nullptr; i = i->next){
        StringList andSplited = split(i->data, " and ");
        bool isAnd = true;
        for (Node<string>* j = andSplited.first; j != nullptr; j = j->next){
            StringList expression = split(j->data, " ");
            string colName1 = split(expression.find(0)->data, ".").find(1)->data;
            int colIndex1 = header.index_word(colName1);
            if (expression.find(2)->data[0] == '\''){
                if (text.find(colIndex1)->data != remove_extra(expression.find(2)->data)){
                    isAnd = false;
                    break;
                }
            }
            else {
                string colName2 = split(expression.find(2)->data, ".").find(1)->data;
                int colIndex2 = header.index_word(colName2);
                if (text.find(colIndex1)->data != text.find(colIndex2)->data){
                    isAnd = false;
                    break;
                }
            }
        }
        if (isAnd){return true;}
    }
    return false;
}

string low_id(const string& command, int lowOn){
    StringList splited = split(command, ";");
    int id = stoi(splited.find(0)->data);
    id -= lowOn;
    splited.find(0)->data = to_string(id);
    string newCommand = splited.join(';');
    return newCommand;
}

void delete_from(const string& schemaName, StringList command){
    StringList tables;
    tables.push_back(command.find(2)->data);
    check_active(schemaName + "/", tables);
    make_active(schemaName + "/", tables);

    string path = schemaName + '/' + command.find(2)->data + '/';
    int currentFile = 1;
    if (command.word_find("WHERE") == command.last){
        while (remove((path + to_string(currentFile) + ".csv").c_str())){
            ++currentFile;
        }

        ifstream inFile("schema.json");
        nlohmann::json schema;
        inFile >> schema;
        StringList columns;
        columns.push_back(command.find(2)->data + "_pk");
        for (auto i: schema["structure"][command.find(2)->data]){
            columns.push_back(i);
        }

        ofstream newFirst(path + "1.csv");
        newFirst.close();

        write_in_csv(path + "1.csv", columns);

        ofstream updateId(path + command.find(2)->data + "_pk_seqquence.txt");
        updateId << "1";
        updateId.close();
        return;
    }

    StringList filter = take_section(command, 4, command.listSize);
    string toSplit = filter.join(' ');
    int diffId = 0;
    do{
        ifstream readFile(path + to_string(currentFile) + ".csv");
        if (!readFile.is_open()){
            break;
        }
        string strHeader;
        readFile >> strHeader;
        StringList header = split(strHeader, ";");
        string line;
        StringList save;
        while(readFile >> line){
            StringList data = split(line, ";");
            if (!check_filter_delete(header, data, toSplit)){
                string temp = low_id(line, diffId);
                save.push_back(temp);
            }
            else {
                ++diffId;
            }
        }
        readFile.close();
        ofstream writeFile(path + to_string(currentFile) + ".csv");
        writeFile << strHeader << endl;
        for (Node<string>* i = save.first; i != nullptr; i = i->next){
            writeFile << i->data << endl;
        }
        writeFile.close();
        ++currentFile;
    }while(true);

    ifstream pkRead(schemaName + '/' + command.find(2)->data + '/' + command.find(2)->data + "_pk_sequence.txt");
    string idStr;
    getline(pkRead, idStr);
    pkRead.close();
    int newID = stoi(idStr) - diffId;
    ofstream pkWrite(schemaName + '/' + command.find(2)->data + '/' + command.find(2)->data + "_pk_sequence.txt");
    pkWrite << newID;
    pkWrite.close();

    make_inactive(schemaName + "/", tables);
}







bool check_filter_select(StringList& header, StringList& text, const string& filter, int currStr, const string& genPath){
    StringList orSplited = split(filter, " or ");
    for (Node<string>* i = orSplited.first; i != nullptr; i = i->next){
        StringList andSplited = split(i->data, " and ");
        bool isAnd = true;
        for (Node<string>* j = andSplited.first; j != nullptr; j = j->next){
            StringList expression = split(j->data, " ");
            string colName1 = split(expression.find(0)->data, ".").find(1)->data;
            int colIndex1 = header.index_word(colName1);
            if (expression.find(2)->data[0] == '\''){
                if (text.find(colIndex1)->data != remove_extra(expression.find(2)->data)){
                    isAnd = false;
                    break;
                }
            }
            else {
                string tabName1 = split(expression.find(0)->data, ".").find(0)->data;
                string path = genPath + tabName1 + "/";
                ifstream forHead1(path + "1.csv");
                string strHead1;
                forHead1 >> strHead1;
                forHead1.close();
                StringList header1 = split(strHead1, ";");
                string colName1 = split(expression.find(0)->data, ".").find(1)->data;
                int colIndex1 = header1.index_word(colName1);
                int currFile = 1;
                int currStrId1 = 1;
                string currStr1;
                do {
                    ifstream check(path + to_string(currFile) + ".csv");
                    if (!check.is_open()){
                        break;
                    }
                    string line;
                    check >> line;
                    while(check >> line && currStrId1 != currStr){
                        ++currStrId1;
                    }
                    if (currStrId1 == currStr){
                        currStr1 = line;
                        check.close();
                        break;
                    }
                    currStr1 = line;
                    check.close();
                    ++currFile;
                }while(true);

                if (currStrId1 != currStr){
                    isAnd = false;
                    break;
                }
                StringList condText1 = split(currStr1, ";");

                string tabName2 = split(expression.find(0)->data, ".").find(0)->data;
                path = genPath + tabName2 + "/";
                ifstream forHead2(path + "1.csv");
                string strHead2;
                forHead2 >> strHead2;
                forHead2.close();
                StringList header2 = split(strHead2, ";");
                string colName2 = split(expression.find(0)->data, ".").find(1)->data;
                int colIndex2 = header2.index_word(colName2);
                currFile = 1;
                int currStrId2 = 1;
                string currStr2;
                do {
                    ifstream check(path + to_string(currFile) + ".csv");
                    if (!check.is_open()){
                        break;
                    }
                    string line;
                    check >> line;
                    while(check >> line && currStrId2 != currStr){
                        ++currStrId2;
                    }
                    if (currStrId1 == currStr){
                        currStr2 = line;
                        check.close();
                        break;
                    }
                    currStr2 = line;
                    check.close();
                    ++currFile;
                }while(true);

                if (currStrId2 != currStr){
                    isAnd = false;
                    break;
                }

                StringList condText2 = split(currStr2, ";");
                if (condText1.find(colIndex1)->data != condText1.find(colIndex2)->data){
                    isAnd = false;
                    break;
                }
            }
        }
        if (isAnd){return true;}
    }
    return false;
}

IntList cnt_rows(StringMatrix& matrix){
    IntList eachCol;
    for (auto i = matrix.firstCol; i != nullptr; i = i->nextCol){
        int cntRow = 0;
        for (auto j = i->nextRow; j != nullptr; j = j->nextRow){
            ++cntRow;
        }
        eachCol.push_back(cntRow);
    }
    return eachCol;
}

void select_from(const string& schemaName, StringList command){
    string genPath = schemaName + '/';

    int whereIndex = command.index_word("WHERE");
    whereIndex = whereIndex > 0 ? whereIndex : command.listSize;
    StringList tables = take_section(command, command.index_word("FROM") + 1, whereIndex);
    for (auto i = tables.first; i != nullptr; i = i->next){
        i->data = remove_extra(i->data);
    }

    check_active(genPath, tables);
    make_active(genPath, tables);

    StringList columns = take_section(command, command.index_word("SELECT") + 1, command.index_word("FROM"));
    for (auto i = columns.first; i != nullptr; i = i->next){
        i->data = remove_extra(i->data);
    }

    StringMatrix toOut;
    int currTable = 0;
    int currCol = 0;
    if (command.word_find("WHERE") == command.last){
        int totalCnt = 1;
        IntList strInTable;
        for (auto i = tables.first; i != nullptr; i = i->next){
            int currFile = 1;
            int cntLines = 0;
            string path = genPath + i->data + '/';
            do{
                ifstream check(path + to_string(currFile) + ".csv");
                if (!check.is_open()){
                    break;
                }
                string line;
                while(check >> line){
                    ++cntLines;
                }
                ++currFile;
            }while(true);
            strInTable.push_back(--cntLines);
            totalCnt *= cntLines;
        }

        for (auto i = tables.first; i != nullptr; i = i->next){
            string path = genPath + i->data + '/';
            totalCnt /= strInTable.find(currTable)->data;
            for (auto j = columns.first; j != nullptr; j = j->next){
                StringList tabNCol = split(j->data, ".");
                if (tabNCol.find(0)->data != i->data){
                    continue;
                }

                ifstream forHead(path + "1.csv");
                string strHeader;
                forHead >> strHeader;
                forHead.close();
                StringList header = split(strHeader, ";");
                int takenId = header.index_word(tabNCol.find(1)->data);
                int currFile = 1;
                do{
                    ifstream readFile(path + to_string(currFile) + ".csv");
                    if (!readFile.is_open()){
                        break;
                    }
                    string line;

                    toOut.push_right(tables.find(currTable)->data + "." + tabNCol.find(1)->data);
                    readFile >> line;
                    while(readFile >> line){
                        StringList splited = split(line, ";");
                        for (int k = 0; k < totalCnt; ++k){
                            toOut.push_down(splited.find(takenId)->data, currCol);
                        }
                    }
                    readFile.close();
                    if (currTable != 0){
                        MatrixNode* currHead = toOut.lastCol;
                        int cntrCurr = 0;
                        for (auto k = currHead->nextRow; k != nullptr; k = k->nextRow){
                            ++cntrCurr;
                        }

                        int cntrFirst = 0;
                        for (auto k = toOut.firstCol; k != nullptr; k = k->nextRow){
                            ++cntrFirst;
                        }

                        for (auto m = 0; m < (cntrFirst / cntrCurr) - 1; ++m){
                            MatrixNode* currRow = currHead->nextRow;
                            for (int k = 0; k < cntrCurr; ++k){
                                toOut.push_down(currRow->data, currCol);
                                currRow = currRow->nextRow;
                            }
                        }
                    }
                    ++currFile;
                }while(true);
                ++currCol;
            }
            ++currTable;
        }
        toOut.print();
        make_inactive(genPath, tables);
        return;
    }

    string filter = take_section(command, command.index_word("WHERE") + 1, command.listSize).join(' ');

    for (auto i = tables.first; i != nullptr; i = i->next){
        string path = genPath + i->data + "/";
        for (auto j = columns.first; j != nullptr; j = j->next){
            StringList tabNCol = split(j->data, ".");
            if (tabNCol.find(0)->data != i->data){
                continue;
            }

            ifstream forHead(path + "1.csv");
            string strHeader;
            forHead >> strHeader;
            forHead.close();
            StringList header = split(strHeader, ";");
            int takenId = header.index_word(tabNCol.find(1)->data);
            int currFile = 1;
            int currStr = 1;
            toOut.push_right(j->data);
            do{
                ifstream readFile(path + to_string(currFile) + ".csv");
                if (!readFile.is_open()){
                    break;
                }
                string line;

                readFile >> line;
                while(readFile >> line){
                    StringList splited = split(line, ";");
                    if (check_filter_select(header, splited, filter, currStr, genPath)){
                        toOut.push_down(splited.find(takenId)->data, currCol);
                    }
                    ++currStr;
                }
                readFile.close();
                ++currFile;
            }while(true);
            ++currCol;
        }
    }

    IntList eachCol = cnt_rows(toOut);
    int totalCnt = 1;
    for (auto i = eachCol.first; i != nullptr; i = i->next){
        if (i->data == 0){
            cout << string("-") * 40 << endl;
            for (auto j = toOut.firstCol; j != nullptr; j = j->nextCol){
                cout << j->data << " ";
            }
            cout << endl << string("-") * 40 << endl;
            return;
        }
        totalCnt *= i->data;
    }

    StringMatrix temp;
    currTable = 0;
    currCol = 0;
    int cntInFirst = eachCol.find(0)->data * totalCnt;
    MatrixNode* prevCol = nullptr;
    totalCnt /= eachCol.find(currCol)->data;
    for (auto i = toOut.firstCol; i != nullptr; i = i->nextCol){
        temp.push_right(i->data);
        for (auto j = i->nextRow; j != nullptr; j = j->nextRow){
            for (int k = 0; k < totalCnt; ++k){
                temp.push_down(j->data, currCol);
            }
        }
        if (i->nextCol != prevCol){
            ++currTable;
            totalCnt /= eachCol.find(currCol)->data;
        }
        ++currCol;
        prevCol = i;
    }

    IntList newEachCol = cnt_rows(temp);
    StringMatrix totalOut;
    currCol = 0;
    for (auto i = temp.firstCol; i != nullptr; i = i->nextCol){
        totalOut.push_right(i->data);
        if (i == temp.firstCol){
            for (auto j = i->nextRow; j != nullptr; j = j->nextRow){
                totalOut.push_down(j->data, currCol);
            }
        }
        else{
            for (int k = 0; k < (newEachCol.find(0)->data / newEachCol.find(currCol)->data); ++k){
                for (auto j = i->nextRow; j != nullptr; j = j->nextRow){
                    totalOut.push_down(j->data, currCol);
                }
            }
        }
        ++currCol;
    }

    totalOut.print();
    make_inactive(genPath, tables);
}

int main()
{
    string schemaName = create_db();

    string command;
    while(true){
        getline(cin, command);
        StringList splited = split(command, " ");
        SQLRequest choice = get_com(splited.find(0)->data);
        switch (choice){
            case SQLRequest::SELECT: select_from(schemaName, splited); break;
            case SQLRequest::INSERT: insert_into(schemaName, splited); break;
            case SQLRequest::DELETE: delete_from(schemaName, splited); break;
            case SQLRequest::END: goto finish;
            case SQLRequest::UNKNOWN: cout << "Wrong command!" << endl; break;
        }
    }
    finish:

    return 0;
}
