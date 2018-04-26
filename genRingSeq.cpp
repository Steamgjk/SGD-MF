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
int n = 2;
int P = 4;
int round_num = 100;
ofstream ofs[10];
int p_or_q[1000][10];
int mms[100][100];
#define ACTION_FILE "./action"
#define STATE_FILE "./state"
int main(int argc, const char * argv[])
{
	if (argc >= 2)
	{
		n = atoi(argv[1]);
	}
	if (argc >= 3)
	{
		P = atoi(argv[2]);
	}
	if (argc >= 4)
	{
		round_num = atoi(argv[3]);
	}
	char filen[100];
	for (int i = 0; i < P; i++ )
	{
		sprintf(filen, "%s-%d", SEQ_FILE, i)
		ofs[i].open(filen, ios::trunc);
		if (!ofs[i].is_open())
		{
			printf("open fail %s\n", filen );
			exit(-1);
		}
	}

	if (!ofs.is_open())
	{
		printf("open fail %s\n", SEQ_FILE );
		exit(-1);
	}

	ofstream actionfs(ACTION_FILE, ios::trunc);
	for (int t = 0; t < round_num; t++)
	{
		for (int i = 0; i < n; i++)
		{
			p_or_q[t][i] = rand() % 2;
			//0 is to right trans Q, 1 is up, trans p
			//the group action
			actionfs << p_or_q[t][i] << " ";

		}
		actionfs << endl;
	}





	std::vector<int> vec(P);
	for (int i = 0; i < P; i++)
	{
		vec[i] = i;
	}

	for (int j = 0; j < n ; j++)
	{
		random_shuffle(vec.begin(), vec.end());
		for (int i = 0; i < P; i++)
		{
			mms[i][j] = vec[i] + j * P;
		}
	}
	//mms[i][j] for node i, the corresponding block_id in group j

	for (int i = 0; i < P; i++)
	{
		for (int j = 0; j < n; j++)
		{
			ofs[i] << mms[i][j] << " ";
		}
		ofs[i] << endl;
	}

	for (int t = 0; t < round_num; t++)
	{
		for (int i = 0; i < P; i++)
		{
			for (int j = 0 ; j < n; j++)
			{
				if (p_or_q[t][j] == 0)
				{
					//0 is to right trans Q, 1 is up, trans p
					mms[i][j] = (mms[i][j] + 1) % (n * P);
				}
				else
				{
					//1 is to up trans P
					mms[i][j] = (mms[i][j] + n * P - 1) % (n * P);
				}
			}
		}
		for (int i = 0; i < P; i++)
		{
			for (int j = 0; j < n; j++)
			{
				ofs[i] << mms[i][j] << " ";
			}
			ofs[i] << endl;
		}

	}



}