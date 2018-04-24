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
#define N  17770 // row number
#define M  2649429 //col number
#define K  40 //主题个数
using namespace std;
int main()
{

    srand(1);
    for (int i = 0; i < N; i++)
    {
        for (int j = 0; j < K; j++)
        {
            printf("%lf ", ((double)rand() / RAND_MAX / 10.0) );
        }
        printf("\n");
    }

}
