#include <iostream>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <unistd.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <cmath>
#include <time.h>
#include <vector>
#include <list>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <map>
using namespace std;
int P = 16;
int round_num = 100;
#define SEQ_FILE "./seq.txt"
int main(int argc, const char * argv[])
{
	if (argc >= 2)
	{
		P = atoi(argv[1]);
	}
	if (argc >= 3)
	{
		round_num = atoi(argv[2]);
	}
	ofstream ofs(SEQ_FILE, ios::trunc);
	if (!ofs.is_open())
	{
		printf("open fail %s\n", SEQ_FILE );
		exit(-1);
	}
	std::vector<int> vec(P);
	for (int i = 0; i < P; i++)
	{
		vec[i] = i;
	}
	int total_num = round_num * P;
	ofs << total_num << endl;
	printf("total_num = %d\n", total_num);
	for (int t = 0; t < round_num; t++)
	{
		random_shuffle(vec.begin(), vec.end());
		for (int i = 0; i < P; i++)
		{
			ofs << i*P + vec[i] << " ";
		}
	}



}