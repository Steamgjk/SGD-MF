#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <iostream>
#include <fstream>
#include <string>
using namespace std;
#define N 71567
#define M 65133
#define K 2
int main()
{
	char filename[100];
	int temp;
	int customer_id;
	char datetime[100];
	int rate;
	char comma;
	int movie_id;
	char line[512];
	//ofstream ofs("netflix_mtx.txt", ios::trunc);
	ifstream ifs("./ml-10M100K/rb.train", ios::in | iso::out);
	ofstream ofs("./ml-10M100K/train.txt", ios::trunc);
	long user_id;
	long movie_id;
	double rate;
	char tmp;
	string line;
	while (!ifs.eof())
	{
		ifs >> user_id >> tmp >> tmp >> movie_id >> tmp >> tmp >> rate >> line;
		long hasn_idx =  (user_id - 1) * M + (movie_id - 1);
		ofs << hasn_idx << rate << endl;
	}




}