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

#define N 71567
#define M 65133
#define K  40 //主题个数
int ITER_NUM  = 2000;
int PORTION_NUM = 8;
double P[N][K];
double Q[K][M];
//#define TEST_NAME "./test_out.txt"
#define FILE_NAME "./mdata/traina-"
#define TEST_NAME "./mdata/testa-"

using namespace std;

//long hashs[1000000];
//double rts[1000000];

int main(int argc, const char * argv[])
{
    ofstream ofs("./rima.txt", ios::trunc);

    if (argc >= 2)
    {
        ITER_NUM = atoi(argv[1]);
    }
    if (argc >= 3)
    {
        PORTION_NUM = atoi(argv[2]);
    }
    ifstream ifs;
    double rmse = 0;

    char fn[100];
    long hash_head = 0;


    char filename[100];
    //for (int i = 0; i < ITER_NUM; i += 10)
    {
        int i = atoi(argv[3]);
        int row_idx = 0;
        int col_idx = 0;
        for (int j = 0 ; j < PORTION_NUM; j++)
        {
            sprintf(filename, "./Rtrack/Pblock-%d-%d", i, j);
            ifs.open(filename, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }

            while (!ifs.eof())
            {
                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> P[row_idx][kk];
                }
                row_idx++;
            }
            ifs.close();
            printf("%s read\n", filename );
            sprintf(filename, "./Rtrack/Qblock-%d-%d", i, j);
            //ifstream ifs1(filename, ios::in | ios::out);
            ifs.open(filename, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open %s\n", filename);
                getchar();
            }
            double temp;
            while (!ifs.eof())
            {

                for (int kk = 0; kk < K; kk++)
                {
                    ifs >> Q[kk][col_idx];
                }

                col_idx++;
            }
            ifs.close();
            printf("%s read\n", filename );

        }
        printf("hash_head = %ld \n", hash_head);


        rmse = 0;
        for (int i = 0; i < 64 ; i++)
        {
            sprintf(fn, "%s%d", TEST_NAME, i);
            ifs.open(fn, ios::in | ios::out);
            if (!ifs.is_open())
            {
                printf("fail to open the file %s\n", TEST_NAME);
                exit(-1);
            }
            int cnt = 0;
            int temp = 0;
            long hash_idx = 0;
            double ra = 0;
            hash_idx = -1;
            while (!ifs.eof())
            {
                ifs >> hash_idx >> ra;
                if (hash_idx >= 0)
                {
                    long row_idx = hash_idx / M;
                    long col_idx = hash_idx % M;
                    double sum = 0;

                    for (int k = 0; k < K; k++)
                    {
                        sum += P[row_idx][k] * Q[k][col_idx];
                        //printf("%lf  %lf\n", P[row_idx][k], Q[k][col_idx]);
                    }

                    rmse += (sum - ra ) * (sum - ra);
                    printf("sum=%lf ra=%lf rmse=%lf\n", sum, ra, rmse );
                    cnt++;
                }

            }
            rmse /= cnt;
            rmse = sqrt(rmse);
            printf("rmse=%lf\n", rmse );
            ifs.close();

        }



    }





    return 0;


}


// 1.7