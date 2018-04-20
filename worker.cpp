//
//  main.cpp
//  linux_socket_api
//
//  Created by bikang on 16/11/2.
//  Copyright (c) 2016年 bikang. All rights reserved.
//

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

using namespace std;

#define BUFFER_SIZE 1024

#define FILE_NAME "./mtx.txt"

#define WORKER_NUM 1
char* remote_ips[10] = {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"};
int remote_ports[10] = {4411, 4412, 4413, 4414};

char* local_ips[10] = {"127.0.0.1", "127.0.0.1", "127.0.0.1", "127.0.0.1"};
int local_ports[10] = {5511, 5512, 5513, 5514};

#define N  10000 //用户数
#define M  10000 //物品数
#define K  20 //主题个数

struct Block
{
    int block_id;
    int data_age;
    int sta_idx;
    int height; //height
    int ele_num;
    vector<double> eles;
    Block()
    {

    }
    Block operator=(Block& bitem)
    {
        block_id = bitem.block_id;
        data_age = bitem.data_age;
        height = bitem.height;
        eles = bitem.eles;
        ele_num = bitem.ele_num;
        sta_idx = bitem.sta_idx;
        return *this;
    }
    void printBlock()
    {

        printf("block_id  %d\n", block_id);
        printf("data_age  %d\n", data_age);
        printf("ele_num  %d\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");

    }
};
struct Updates
{
    int block_id;
    int clock_t;
    int ele_num;
    vector<double> eles;
    Updates()
    {

    }
    Updates operator=(Updates& uitem)
    {
        block_id = uitem.block_id;
        clock_t = uitem.clock_t;
        ele_num = uitem.ele_num;
        eles = uitem.eles;
        return *this;
    }

    void printUpdates()
    {

        printf("update block_id %d\n", block_id );
        printf("clock_t  %d\n", clock_t);
        printf("ele size %ld\n", ele_num);
        for (int i = 0; i < eles.size(); i++)
        {
            printf("%lf\t", eles[i]);
        }
        printf("\n");

    }
};
struct Block Pblock;
struct Block Qblock;
struct Updates Pupdt;
struct Updates Qupdt;

bool canSend = false;
bool hasRecved = false;

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void submf(double *minR, Block& minP, Block& minQ, Updates& updateP, Updates& updateQ,  int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02);

void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len);
int thread_id = -1;
int main(int argc, const char * argv[])
{

    thread_id = atoi(argv[1]);
    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();

    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();
    //double* minR = (double*)malloc(sizeof(double) * 1000);
    while (1 == 1)
    {
        if (hasRecved)
        {
            //SGD
            int row_sta_idx = Pblock.sta_idx;
            int row_len = Pblock.height;
            int col_sta_idx = Qblock.sta_idx;
            int col_len = Qblock.height;
            int ele_num = row_len * col_len;
            //printf("ele_num = %d   size = %ld\n", ele_num, sizeof(double) * ele_num);
            //getchar();
            double* minR = (double*)malloc(sizeof(double) * ele_num);
            for (int i = 0; i < ele_num; i++)
            {
                minR[i] = 0;
            }
            //printf("okkkk minR=%p\n", minR);
            getMinR(minR, row_sta_idx, row_len, col_sta_idx, col_len);
            /*
            for (int i = 0; i < ele_num; i++)
            {
                printf("%lf\t", minR[i] );
            }
            **/
            //printf("fin  before minR  %p\n", minR);
            submf(minR, Pblock, Qblock, Pupdt, Qupdt, K);
            //printf("minR = %p\n", minR);
            free(minR);

            canSend = true;
            hasRecved = false;
        }
        else
        {
            //printf("[Id:%d] has not received...\n", thread_id );
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }
    /** Test
    int row_sta_idx = 13;
    int row_len = 3;
    int col_sta_idx = 1;
    int col_len = 4;
    int ele_num = row_len * col_len;
    double* minR = (double*)malloc(sizeof(double) * ele_num);
    getMinR(minR, row_sta_idx, row_len, col_sta_idx, col_len);
    for (int i = 0 ; i < row_len; i++)
    {
    for (int j = 0; j < col_len; j++)
    {
    printf("%lf ", minR[i * col_len + j] );
    }
    printf("\n");
    }
    return 0;
    **/
}

void getMinR(double* minR, int row_sta_idx, int row_len, int col_sta_idx, int col_len)
{
    //printf("row_sta_idx = %d row_len=%d col_sta_idx=%d  col_len = %d\n", row_sta_idx, row_len, col_sta_idx, col_len);

    ifstream ifs(FILE_NAME);
    string temp;
    for (int i = 0; i < row_sta_idx; i++)
    {
        getline(ifs, temp);
        //cout << "temp:\t" << temp << endl;
    }
    //printf("check cc 1\n");
    int line_no = row_sta_idx;
    double temp_db;
    int total_num = row_len * col_len;
    int cnt = 0;
    //printf("check cc 2\n");

    for (int i = row_sta_idx; i < row_sta_idx + row_len; i++)
    {
        for (int j = 0 ; j < col_sta_idx; j++)
        {
            ifs >> temp_db;
            //cout << "tf " << temp_db << endl;
        }
        //cout << endl;
        for (int j = col_sta_idx; j < col_sta_idx + col_len; j++)
        {
            ifs >> minR[cnt];
            //cout << "minR " << minR[cnt] << endl;
            cnt++;
        }
        //cout << endl;
        //getchar();
        for (int j = col_sta_idx + col_len; j < M; j++)
        {
            ifs >> temp_db;
            //cout << "tfb " << temp_db << endl;
            //getchar();
        }
        //getchar();
    }
    //printf("Returned  \n");
}

void submf(double * minR, Block & minP, Block & minQ, Updates & updateP, Updates & updateQ, int minK, int steps, float alpha , float beta)
{
    //printf("begin submf\n");
    double error = 0;
    int minN = minP.height;
    int minM = minQ.height;

    int Psz =  minP.height * minK;
    int Qsz = minQ.height * minK;
    //printf("Psz =%d Qsz =%d\n", Psz, Qsz);
    updateP.eles.resize(Psz);
    updateP.ele_num = Psz;
    updateQ.eles.resize(Qsz);
    updateQ.ele_num = Qsz;
    int ii = 0;
    for (ii = 0; ii < Psz; ii++)
    {
        updateP.eles[ii] = 0;
    }
    for ( ii = 0; ii < Qsz; ii++)
    {
        updateQ.eles[ii] = 0;
    }
    vector<double> originalP = minP.eles;
    vector<double> originalQ = minQ.eles;
    vector<int> vshuf(minM * minN);
    for (int i = 0; i < minM * minN; i++)
    {
        vshuf[i] = i;
    }
    random_shuffle(vshuf.begin(), vshuf.end());//迭代器
    /*
    for (int ii = 0; ii < minN * minM; ii++)
    {
        printf("%d ", vshuf[ii] );
    }
    printf("\n");
    **/
    //for (int step = 0; step < steps; ++step)
    {

// should be updated one by one
        //for (int i = 0; i < minN; ++i)
        {
            //for (int j = 0; j < minM; ++j)
            for (int ii = 0; ii < minN * minM; ii++)
            {
                int idx = vshuf[ii];
                int i = idx / minM;
                int j = idx % minM;
                if (minR[i * minM + j] > 0)
                {

                    //printf("idx = %d\n", i * minM + j );
                    //这里面的error 就是公式6里面的e(i,j)
                    error = minR[i * minM + j];

                    //printf("error = %lf\n", error );
                    for (int k = 0; k < minK; ++k)
                    {
                        //error_m -= P[i * minK + k] * Q[k * minM + j];
                        error -= minP.eles[i * minK + k] * minQ.eles[j * minK + k];
                    }

                    //更新公式6
                    for (int k = 0; k < minK; ++k)
                    {
                        //printf("minP sz = %ld minQ sz =%ld updt sz %ld updt sz %ld\n i*minK+k=%d  j*minK+k=%d\n", minP.eles.size(), minQ.eles.size(), updateP.eles.size(), updateQ.eles.size(), i * minK + k, j * minK + k );
                        //updateP.eles[i * minK + k] += alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]);
                        //updateQ.eles[j * minK + k] += alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]);
                        minP.eles[i * minK + k] += alpha * (2 * error * minQ.eles[j * minK + k] - beta * minP.eles[i * minK + k]);
                        minQ.eles[j * minK + k] += alpha * (2 * error * minP.eles[i * minK + k] - beta * minQ.eles[j * minK + k]);
                    }


                }
            }
            for (int i = 0; i < originalP.size(); i++)
            {
                updateP.eles[i] = minP.eles[i] - originalP[i];
            }
            for (int j = 0; j < originalQ.size(); j++)
            {
                updateQ.eles[j] = minQ.eles[j] - originalQ[j];
            }

        }

        //printf("end submf  minR=%p\n", minR);
    }




}
int wait4connection(char*local_ip, int local_port)
{
    int fd = socket(PF_INET, SOCK_STREAM , 0);
    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(local_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, local_ip, &address.sin_addr);
    //设置socket buffer大小
    //int recvbuf = 4096;
    //int len = sizeof( recvbuf );
    //setsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, sizeof( recvbuf ) );
    //getsockopt( fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, ( socklen_t* )&len );
    //printf( "the receive buffer size after settting is %d\n", recvbuf );
    //绑定ip和端口
    int check_ret = -1;
    do
    {
        printf("binding...\n");
        check_ret = bind(fd, (struct sockaddr*)&address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret >= 0);
    printf("bind ok\n");
    //创建监听队列，用来存放待处理的客户连接
    check_ret = listen(fd, 5);
    assert(check_ret >= 0);

    struct sockaddr_in addressClient;
    socklen_t clientLen = sizeof(addressClient);

    printf("thread %d listening at %s %d\n", thread_id, local_ip, local_port );
    //接受连接，阻塞函数
    int connfd = accept(fd, (struct sockaddr*)&addressClient, &clientLen);
    return connfd;

}
void sendTd(int send_thread_id)
{
    printf("send_thread_id=%d\n", send_thread_id);
    char* remote_ip = remote_ips[send_thread_id];
    int remote_port = remote_ports[send_thread_id];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    int sendbuf = 4096;
    int len = sizeof( sendbuf );
    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    assert(check_ret >= 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    while (1 == 1)
    {
        if (!canSend)
        {
            //printf("Td %d cannotSend...\n", thread_id );
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
        else
        {
            //printf("Td:%d cansend\n", thread_id );
            size_t struct_sz = sizeof(Pupdt);
            size_t data_sz = sizeof(double) * Pupdt.eles.size();
            char* buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Pupdt), struct_sz);
            memcpy(buf + struct_sz, (char*) & (Pupdt.eles[0]), data_sz);
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Id:%d] send success \n", thread_id);
            }
            free(buf);

            struct_sz = sizeof(Qupdt);
            data_sz = sizeof(double) * Qupdt.eles.size();
            buf = (char*)malloc(struct_sz + data_sz);
            memcpy(buf, &(Qupdt), struct_sz);
            memcpy(buf + struct_sz , (char*) & (Qupdt.eles[0]), data_sz);
            ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Id:%d] send success \n", thread_id);
            }
            free(buf);
            //printf("Here we pause...\n");
            //getchar();
            canSend = false;
        }
    }

}
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    while (1 == 1)
    {
        //printf("check 0\n");
        size_t expected_len = sizeof(Pblock);
        char* sockBuf = (char*)malloc(expected_len + 100);
        size_t cur_len = 0;
        int ret = 0;
        //printf("check 1  expected_len=%ld sockBuf=%p\n", expected_len, sockBuf);

        while (cur_len < expected_len)
        {
            //printf("cur_len = %ld  expected_len = %ld\n", cur_len, expected_len);
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            //printf("check 1.5\n");
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        //ret = recv(connfd, sockBuf, expected_len, 0);
        //printf("check 2\n");
        struct Block* pb = (struct Block*)(void*)sockBuf;
        Pblock.block_id = pb->block_id;
        Pblock.data_age = pb->data_age;
        Pblock.sta_idx = pb->sta_idx;
        Pblock.height = pb->height;
        Pblock.ele_num = pb->ele_num;
        Pblock.eles.resize(pb->ele_num);
        //printf("check 3\n");
        //free(sockBuf);

        size_t data_sz = sizeof(double) * (Pblock.ele_num);
        sockBuf = (char*)malloc(data_sz);
        //printf("check 4\n");
        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        //printf("check 5\n");
        double* data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Pblock.ele_num; i++)
        {
            Pblock.eles[i] = data_eles[i];
        }
        free(data_eles);
        //printf("[ID:%d] get Pblock %d\n", thread_id, Pblock.block_id);
        //Pblock.printBlock();

        //printf("Here recv pause...\n");
        //getchar();

        expected_len = sizeof(Pblock);
        sockBuf = (char*)malloc(expected_len);
        cur_len = 0;
        ret = 0;
        while (cur_len < expected_len)
        {
            ret = recv(connfd, sockBuf + cur_len, expected_len - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }
        struct Block* qb = (struct Block*)(void*)sockBuf;
        Qblock.block_id = qb->block_id;
        Qblock.data_age = qb->data_age;
        Qblock.sta_idx = qb->sta_idx;
        Qblock.height = qb->height;
        Qblock.ele_num = qb-> ele_num;
        Qblock.eles.resize(qb->ele_num);
        free(sockBuf);

        data_sz = sizeof(double) * (Qblock.ele_num);
        sockBuf = (char*)malloc(data_sz);

        cur_len = 0;
        ret = 0;
        while (cur_len < data_sz)
        {
            ret = recv(connfd, sockBuf + cur_len, data_sz - cur_len, 0);
            if (ret < 0)
            {
                printf("Mimatch!\n");
            }
            cur_len += ret;
        }

        data_eles = (double*)(void*)sockBuf;
        for (int i = 0; i < Qblock.ele_num; i++)
        {
            Qblock.eles[i] = data_eles[i];
        }
        free(data_eles);

        //printf("[ID:%d] get Qblock %d\n", thread_id, Qblock.block_id);
        //Qblock.printBlock();

        hasRecved = true;




    }
}


