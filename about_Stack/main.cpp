#include <iostream>
#include <string>
#include <algorithm>
using namespace std;
int PowNum(int n)
    {
     int re = 1;
       for(int i = n ; i >= 1 ; i --)
       {
           re = re*10;
       }
       return re -1;
    }
int main()
{
     string str1 = "hello world";
     reverse(str1.begin(),str1.begin()+6);

     cout << str1;
    return 0;
}
