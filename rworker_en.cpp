//
//  main.cpp
//  linux_socket_api
//
//  Created by Jinkun Geng on 18/5/2.
//  Copyright (c) 2018年 Jinkun Geng. All rights reserved.
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
#include <queue>
#include <map>
#include <thread>
#include <chrono>
#include <algorithm>
#include <mutex>
#include <atomic>
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <queue>
using namespace std;


#define WORKER_TD 32
#define ACTION_NAME "./action"
#define STATE_NAME "./state"

char* local_ips[10] = {"12.12.10.12", "12.12.10.15", "12.12.10.16", "12.12.10.17"};
int local_ports[10] = {5511, 5512, 5513, 5514};


//#define FILE_NAME "./traina.txt"
//#define TEST_NAME "./testa.txt"

#define FILE_NAME "./data/TrainingMap-"
#define TEST_NAME "./data/TestMap-"
#define N 1000000
#define M 1000000
#define K  100 //主题个数

//#define N 71567
//#define M 65133
//#define K  40 //主题个数
#define CAP 30
#define SEQ_LEN 1000
#define QU_LEN 5000
#define ThreshIter 100

int GROUP_NUM = 2;
int DIM_NUM = 4;
int WORKER_NUM = 2;
int CACHE_NUM = 20;

int process_qu[WORKER_TD][SEQ_LEN];
int process_head[WORKER_TD];
int process_tail[WORKER_TD];

struct Block
{
    int block_id;
    int data_age;
    int sta_idx;
    int height; //height
    int ele_num;
    bool isP;
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

struct Block Pblocks[CAP];
struct Block Qblocks[CAP];

struct timeval start, stop, diff;

int states[QU_LEN];
int actions[QU_LEN];

int to_send[QU_LEN];
int to_send_head, to_send_tail;

int to_recv[QU_LEN];
int to_recv_head, to_recv_tail;

int has_processed;
std::map<long, double> TrainMaps[100][100];
std::map<long, double> TestMaps[100][100];
//0 is to right trans Q, 1 is up, trans p

int wait4connection(char*local_ip, int local_port);
void sendTd(int send_thread_id);
void recvTd(int recv_thread_id);
void readData(int data_thread_id);

void partitionP(int portion_num,  Block* Pblocks);
void partitionQ(int portion_num,  Block* Qblocks);
void submf(double *minR, Block& minP, Block& minQ, int minK, int steps = 50, float alpha = 0.0002, float beta = 0.02);
void WriteLog(Block&Pb, Block&Qb);
void LoadActionConfig(char* fn);
void LoadStateConfig(char* fn);
void getTestMap(map<long, double>& TestMap, int block_id);
void getBlockRates(map<long, double>& BlockMap, int block_id);
void SGD_MF();
double CalcRMSE(map<long, double>& RTestMap, Block& minP, Block& minQ);
void LoadData(int pre_read);

int thread_id = -1;
int p_block_idx;
int q_block_idx;
int main(int argc, const char * argv[])
{
    srand(time(0));
    thread_id = atoi(argv[1]);
    WORKER_NUM = atoi(argv[2]);
    DIM_NUM = GROUP_NUM * WORKER_NUM;
    to_send_head = to_recv_tail = to_recv_tail = to_recv_head = has_processed = 0;

    LoadActionConfig(ACTION_NAME);
    char state_name[100];
    sprintf(state_name, "%s-%d", state_name, thread_id);
    LoadStateConfig(state_name);
    LoadData(CACHE_NUM);
    std::thread data_read_thread(readData, thread_id);
    data_read_thread.detach();

    std::thread send_thread(sendTd, thread_id);
    send_thread.detach();
    std::thread recv_thread(recvTd, thread_id);
    recv_thread.detach();

    partitionP(DIM_NUM, Pblocks);
    partitionQ(DIM_NUM, Qblocks);


    int block_to_process;
    int action_to_process;
    int action = 0;
    int state_idx = 0;
    std::vector<int> p_to_process(GROUP_NUM);
    std::vector<int> q_to_process(GROUP_NUM);
    std::vector<bool> send_this_p(GROUP_NUM);
    //Init Mark
    while (1 == 1)
    {
        for (int i = 0; i < GROUP_NUM; i++)
        {
            block_to_process = states[state_idx];
            action = actions[state_idx];
            p_to_process[i] = block_to_process / (DIM_NUM);
            q_to_process[i] = block_to_process % (DIM_NUM);
            //0 is to right trans Q, 1 is up, trans p
            if (action == 0)
            {
                send_this_p[i] = true;
            }
            else
            {
                send_this_p[i] = false;
            }
            state_idx++;
        }

        //random_shuffle(p_to_process.begin(), p_to_process.end());

        for (int i = 0; i < GROUP_NUM; i++)
        {

            p_block_idx = p_to_process[i];
            q_block_idx = q_to_process[i];
            printf("pidx = %d  qidx=%d to_recv_head=%d to_recv_tail=%d\n", p_block_idx, q_block_idx, to_recv_head, to_recv_tail );
            SGD_MF();
            if (send_this_p[i] == true)
            {
                to_send[to_send_tail] = p_to_process[i];
                to_send_tail = (to_send_tail + 1) % QU_LEN;
            }
            else
            {
                to_send[to_send_tail] = q_to_process[i];
                to_send_tail = (to_send_tail + 1) % QU_LEN;
            }
            has_processed++;
            while (has_processed < to_recv_head)
            {
                //Wait
                printf("to recv\n");
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }

        }


    }
}

void LoadActionConfig(char* fn)
{
    //Should init both send_action and recv_action //same
    /*
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", fn);
        exit(-1);
    }
    int cnt = 0;
    int act = 0;
    while (!ifs.eof())
    {
        ifs >> act;
        actions[cnt] = act;
        cnt++;
    }
    **/
    int cnt = 0;
    for (int i = 0; i < SEQ_LEN; i++ )
    {
        for (int gp = 0; gp < GROUP_NUM; gp++)
        {
            actions[cnt] = gp % 2;
        }
    }

}
void LoadStateConfig(char* fn)
{
    //Should init both to_send and to_recv //recv gained from action
    /*
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", fn);
        exit(-1);
    }

    int st = 0;
    int cnt = 0;
    while (!ifs.eof())
    {
        ifs >> st;
        states[cnt] = st;
        cnt++;
    }
    **/
    for (int gp = 0; gp < GROUP_NUM; gp++)
    {
        int row = thread_id * GROUP_NUM + gp;
        int col = thread_id * GROUP_NUM + gp;
        states[gp] = row * DIM_NUM + col;
        printf("state[%d] %d\n", gp, states[gp] );
    }
    for (size_t i = 0; i < SEQ_LEN; i++ )
    {
        for (int gp = 0 ; gp < GROUP_NUM; gp++)
        {
            //0 is to right ,send Q and will  recv Q; 1 is up, send p and will  recv P
            int loc = i * GROUP_NUM + gp;
            if (actions[loc] == 0)
            {
                to_send[loc] = states[loc] % DIM_NUM;
                to_recv[loc] = (to_send[loc] + GROUP_NUM) % DIM_NUM;
                states[loc] = (states[loc] / DIM_NUM) * DIM_NUM + ((states[loc] + 1) % DIM_NUM);
            }
            else
            {
                to_send[loc] = states[loc] / DIM_NUM;
                to_recv[loc] = (to_send[loc]  + DIM_NUM - GROUP_NUM) % DIM_NUM;
                states[loc] = ((states[loc] / DIM_NUM + DIM_NUM - 1) % DIM_NUM) * DIM_NUM + (states[loc] % DIM_NUM);
            }
        }

    }

}
void LoadData(int pre_read)
{
    char fn[100];
    long hash_id;
    double rate;
    long cnt = 0;
    for (int i = 0; i < pre_read; i++)
    {
        int data_idx = states[i];
        int row = data_idx / DIM_NUM;
        int col = data_idx % DIM_NUM;

        if (TrainMaps[row][col].size() != 0)
        {
            continue;
        }
        sprintf(fn, "%s%d", FILE_NAME, data_idx);
        printf("fn=%s\n", fn );
        ifstream ifs(fn);
        if (!ifs.is_open())
        {
            printf("fail to open %s\n", fn );
            exit(-1);
        }
        cnt = 0;
        while (!ifs.eof())
        {
            ifs >> hash_id >> rate;
            TrainMaps[row][col].insert(pair<long, double>(hash_id, rate));
            cnt++;
            if (cnt % 100000 == 0)
            {
                printf("Train cnt = %ld\n", cnt);
            }
        }
        sprintf(fn, "%s%d", TEST_NAME, data_idx);
        printf("fn=%s\n", fn );
        ifstream ifs2(fn);
        cnt = 0;
        if (!ifs2.is_open())
        {
            printf("fail to open %s\n", fn );
            exit(-1);
        }
        while (!ifs2.eof())
        {
            ifs2 >> hash_id >> rate;
            TestMaps[row][col].insert(pair<long, double>(hash_id, rate));
            cnt++;
            if (cnt % 100000 == 0)
            {
                printf("Test cnt = %ld\n", cnt);
            }
        }

    }
}
void readData(int data_thread_id)
{
    int head_idx = 0;
    int tail_idx = CACHE_NUM;
    char fn[100];
    long hash_id;
    double rate;
    long cnt = 0;
    while (head_idx < to_send_tail)
    {
        if (tail_idx >= QU_LEN)
        {
            break;
        }
        int data_idx = states[tail_idx];
        int row = data_idx / DIM_NUM;
        int col = data_idx % DIM_NUM;

        if (TrainMaps[row][col].size() != 0)
        {
            continue;
        }
        sprintf(fn, "%s%d", FILE_NAME, data_idx);
        ifstream ifs(fn);
        if (!ifs.is_open())
        {
            printf("fail to open %s\n", fn );
            exit(-1);
        }
        cnt = 0;
        while (!ifs.eof())
        {
            ifs >> hash_id >> rate;
            TrainMaps[row][col].insert(pair<long, double>(hash_id, rate));
            cnt++;
            if (cnt % 100000 == 0)
            {
                printf("Train cnt = %ld\n", cnt);
            }
        }
        sprintf(fn, "%s%d", TEST_NAME, data_idx);
        ifstream ifs2(fn);
        cnt = 0;
        if (!ifs2.is_open())
        {
            printf("fail to open %s\n", fn );
            exit(-1);
        }
        while (!ifs2.eof())
        {
            ifs2 >> hash_id >> rate;
            TestMaps[row][col].insert(pair<long, double>(hash_id, rate));
            cnt++;
            if (cnt % 100000 == 0)
            {
                printf("Test cnt = %ld\n", cnt);
            }
        }
        tail_idx++;
        data_idx = states[head_idx];
        row = data_idx / DIM_NUM;
        col = data_idx % DIM_NUM;
        TrainMaps[row][col].clear();
        TestMaps[row][col].clear();
        head_idx++;
    }

}
void getBlockRates(map<long, double>& BlockMap, int block_id)
{
    char fn[100];
    sprintf(fn, "BlockRate-%d", block_id);
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", fn);
        exit(-1);
    }
    long hash_id;
    double rate;
    while (!ifs.eof())
    {
        ifs >> hash_id >> rate;
        BlockMap.insert(pair<long, double>(hash_id, rate));
    }
}

void getTestMap(map<long, double>& TestMap, int block_id)
{
    char fn[100];
    sprintf(fn, "TestRate-%d", block_id);
    ifstream ifs(fn);
    if (!ifs.is_open())
    {
        printf("fail to open the file %s\n", fn);
        exit(-1);
    }
    long hash_id;
    double rate;
    while (!ifs.eof())
    {
        ifs >> hash_id >> rate;
        TestMap.insert(pair<long, double>(hash_id, rate));
    }
}


void WriteLog(Block&Pb, Block&Qb)
{
    char fn[100];
    sprintf(fn, "Pblock-%d", Pb.block_id);
    ofstream pofs(fn, ios::trunc);
    for (int h = 0; h < Pb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            pofs << Pb.eles[h * K + j] << " ";
        }
        pofs << endl;
    }
    sprintf(fn, "Qblock-%d", Qb.block_id);
    ofstream qofs(fn, ios::trunc);
    for (int h = 0; h < Qb.height; h++)
    {
        for (int j = 0; j < K; j++)
        {
            qofs << Qb.eles[h * K + j] << " ";
        }
        qofs << endl;
    }
}



double CalcRMSE(map<long, double>& RTestMap, Block& minP, Block& minQ)
{
    double rmse = 0;
    int cnt = 0;
    map<long, double>::iterator iter;
    int positve_cnt = 0;
    int negative_cnt = 0;
    long row_sta_idx = minP.sta_idx;
    long col_sta_idx = minQ.sta_idx;
    //printf("Psz = %ld  Qsz= %ld row_sta_idx=%ld, col_sta_idx=%ld\n", minP.ele_num, minQ.ele_num, row_sta_idx, col_sta_idx );
    for (iter = RTestMap.begin(); iter != RTestMap.end(); iter++)
    {
        long real_hash_idx = iter->first;
        long row_idx = real_hash_idx / M - row_sta_idx;
        long col_idx = real_hash_idx % M - col_sta_idx;
        double sum = 0;

        for (int k = 0; k < K; k++)
        {
            //printf("hashs=%ld row_idx=%ld col_idx=%ld M=%ld row_sta_idx=%ld col_sta_idx=%ld Pidx = %ld  Qidx = %ld pbid=%d qid=%d\n", real_hash_idx, row_idx, col_idx, M, row_sta_idx, col_sta_idx, row_idx * K + k, col_idx * K + k, minP.block_id, minQ.block_id );
            sum += minP.eles[row_idx * K + k] * minQ.eles[col_idx * K + k];
        }

        rmse += (sum - iter->second) * (sum - iter->second);
        cnt++;
    }
    if (cnt != 0)
    {
        rmse /= cnt;
        rmse = sqrt(rmse);
    }
    else
    {
        rmse = 0;
    }
    return rmse;
}

void CalcSGD(int worker_td)
{
    int row_sta_idx = Pblocks[p_block_idx].sta_idx;
    int col_sta_idx = Qblocks[p_block_idx].sta_idx;
    int row_len = Pblocks[p_block_idx].height;
    int col_len = Qblocks[q_block_idx].height;
    int row_unit_len = row_len / WORKER_TD;
    int col_unit_len = col_len / WORKER_TD;

    while (process_tail[worker_td] <= process_head[worker_td])
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }



}
void SGD_MF()
{
    double error = 0;
    int row_sta_idx = Pblocks[p_block_idx].sta_idx;
    int col_sta_idx = Qblocks[p_block_idx].sta_idx;
    int row_len = Pblocks[p_block_idx].height;
    int col_len = Qblocks[q_block_idx].height;
    int Psz =  Pblocks[p_block_idx].height * K;
    int Qsz = Qblocks[q_block_idx].height * K;

    //printf("row_len=%d col_len=%d\n", row_len, col_len );

    double old_rmse = CalcRMSE(TestMaps[p_block_idx][q_block_idx], Pblocks[p_block_idx], Qblocks[q_block_idx]);
    double new_rmse = old_rmse;

    int iter_cnt = 0;

    while ( new_rmse > 0.999 * old_rmse )
    {
        vector<double> oldP = Pblocks[p_block_idx].eles;
        vector<double> oldQ = Qblocks[q_block_idx].eles;
        for (int c_row_idx = 0; c_row_idx < row_len; c_row_idx++)
        {
            long i = c_row_idx;
            long j = rand() % col_len;

            long real_row_idx = i + row_sta_idx;
            long real_col_idx = j + col_sta_idx;
            long real_hash_idx = real_row_idx * M + real_col_idx;

            map<long, double>::iterator iter;
            iter = TrainMaps[p_block_idx][q_block_idx].find(real_hash_idx);
            if (iter != TrainMaps[p_block_idx][q_block_idx].end())
            {
                error = iter->second;
                for (int k = 0; k < K; ++k)
                {
                    error -= oldP[i * K + k] * oldQ[j * K + k];
                }
                for (int k = 0; k < K; ++k)
                {
                    Pblocks[p_block_idx].eles[i * K + k] += 0.002 * (error * oldQ[j * K + k] - 0.05 * oldP[i * K + k]);
                    Qblocks[q_block_idx].eles[j * K + k] += 0.002 * (error * oldP[i * K + k] - 0.05 * oldQ[j * K + k]);
                }
            }
        }
        iter_cnt++;
        new_rmse = CalcRMSE(TestMaps[p_block_idx][q_block_idx], Pblocks[p_block_idx], Qblocks[q_block_idx]);
        if (iter_cnt % 100 == 0)
        {
            printf("old_rmse = %lf new_rmse=%lf itercnt=%d\n", old_rmse, new_rmse, iter_cnt );
        }

        if (iter_cnt > 10)
        {
            break;
        }
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
    int right_idx = (send_thread_id + 1) % WORKER_NUM;
    char* remote_ip = local_ips[right_idx];
    int remote_port = local_ports[right_idx];

    int fd;
    int check_ret;
    fd = socket(PF_INET, SOCK_STREAM , 0);
    assert(fd >= 0);

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    //转换成网络地址
    address.sin_port = htons(remote_port);
    address.sin_family = AF_INET;
    //地址转换
    inet_pton(AF_INET, remote_ip, &address.sin_addr);
    do
    {
        //printf("Trying to connect to %s %d\n", remote_ip, remote_port);
        check_ret = connect(fd, (struct sockaddr*) &address, sizeof(address));
        //std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
    while (check_ret < 0);
    //发送数据
    printf("connect to %s %d\n", remote_ip, remote_port);
    int send_cnt = 0;
    while (1 == 1)
    {
        printf("to_send_head=%d to_send_tail=%d\n", to_send_head, to_send_tail );
        if (to_send_head < to_send_tail)
        {
            int block_idx = to_send[to_send_head];
            int block_p_or_q = actions[to_send_head];
            //0 is to right trans Q, 1 is up, trans p
            size_t struct_sz = sizeof(Block);
            size_t data_sz = 0;
            char*buf = NULL;
            if (block_p_or_q == 0)
            {
                //send q
                data_sz = sizeof(double) * Qblocks[block_idx].eles.size();
                buf = (char*)malloc(struct_sz + data_sz);
                memcpy(buf, &(Qblocks[block_idx]), struct_sz);
                memcpy(buf + struct_sz, (char*) & (Qblocks[block_idx].eles[0]), data_sz);
            }
            else
            {
                //send p
                data_sz = sizeof(double) * Pblocks[block_idx].eles.size();
                buf = (char*)malloc(struct_sz + data_sz);
                memcpy(buf, &(Pblocks[block_idx]), struct_sz);
                memcpy(buf + struct_sz, (char*) & (Pblocks[block_idx].eles[0]), data_sz);
            }
            int ret = send(fd, buf, (struct_sz + data_sz), 0);
            if (ret >= 0 )
            {
                printf("[Id:%d] send success \n", thread_id);
            }
            free(buf);

            to_send_head = (to_send_head + 1) % QU_LEN;

        }
    }

}
void recvTd(int recv_thread_id)
{
    printf("recv_thread_id=%d\n", recv_thread_id);
    int connfd = wait4connection(local_ips[recv_thread_id], local_ports[recv_thread_id] );

    printf("[Td:%d] worker get connection\n", recv_thread_id);
    int cnt = 0;
    size_t expected_len = sizeof(Block);
    char* blockBuf = NULL;
    char* dataBuf = NULL;
    size_t cur_len = 0;
    int ret = 0;
    while (1 == 1)
    {
        //if (to_recv_head < to_recv_tail)
        {
            int block_idx = to_recv[to_recv_head];
            int block_p_or_q = actions[to_recv_head];
            //0 is to right trans/recv Q, 1 is up, trans p
            cur_len = 0;
            ret = 0;

            while (cur_len < expected_len)
            {
                ret = recv(connfd, blockBuf + cur_len, expected_len - cur_len, 0);
                if (ret < 0)
                {
                    printf("Mimatch!\n");
                }
                cur_len += ret;
            }
            struct Block* pb = (struct Block*)(void*)blockBuf;
            size_t data_sz = sizeof(double) * (pb->ele_num);
            char* dataBuf = (char*)malloc(data_sz);

            cur_len = 0;
            ret = 0;
            while (cur_len < data_sz)
            {
                ret = recv(connfd, dataBuf + cur_len, data_sz - cur_len, 0);
                if (ret < 0)
                {
                    printf("Mimatch!\n");
                }
                cur_len += ret;
            }
            double* data_eles = (double*)(void*)dataBuf;


            if (block_p_or_q == 0)
            {
                // recv q
                Qblocks[block_idx].block_id = pb->block_id;
                Qblocks[block_idx].sta_idx = pb->sta_idx;
                Qblocks[block_idx].height = pb->height;
                Qblocks[block_idx].ele_num = pb->ele_num;
                Qblocks[block_idx].eles.resize(pb->ele_num);
                Qblocks[block_idx].isP = pb->isP;
                for (int i = 0; i < pb->ele_num; i++)
                {
                    Qblocks[block_idx].eles[i] = data_eles[i];
                }
            }
            else
            {
                Pblocks[block_idx].block_id = pb->block_id;
                Pblocks[block_idx].sta_idx = pb->sta_idx;
                Pblocks[block_idx].height = pb->height;
                Pblocks[block_idx].ele_num = pb->ele_num;
                Pblocks[block_idx].eles.resize(pb->ele_num);
                Pblocks[block_idx].isP = pb->isP;
                for (int i = 0; i < pb->ele_num; i++)
                {
                    Pblocks[block_idx].eles[i] = data_eles[i];
                }
            }
            free(blockBuf);
            free(dataBuf);
            to_recv_head = (to_recv_head + 1) % QU_LEN;
        }
    }
}

void partitionP(int portion_num,  Block* Pblocks)
{
    int i = 0;
    int height = N / portion_num;
    int last_height = N - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Pblocks[i].block_id = i;
        Pblocks[i].data_age = 0;
        Pblocks[i].eles.clear();
        Pblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Pblocks[i].height = last_height;
        }
        Pblocks[i].sta_idx = sta_idx;
        Pblocks[i].ele_num = Pblocks[i].height * K;
        Pblocks[i].eles.resize(Pblocks[i].ele_num);
    }

}

void partitionQ(int portion_num,  Block* Qblocks)
{
    int i = 0;
    int height = M / portion_num;
    int last_height = M - (portion_num - 1) * height;

    for (i = 0; i < portion_num; i++)
    {
        Qblocks[i].block_id = i;
        Qblocks[i].data_age = 0;
        Qblocks[i].eles.clear();
        Qblocks[i].height = height;
        int sta_idx = i * height;
        if ( i == portion_num - 1)
        {
            Qblocks[i].height = last_height;
        }
        Qblocks[i].sta_idx = sta_idx;
        Qblocks[i].ele_num = Qblocks[i].height * K;
        Qblocks[i].eles.resize(Qblocks[i].ele_num);

    }

}
