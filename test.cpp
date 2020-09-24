#include <iostream>

using namespace std;

int main()
{
    int a = 1;
    int *p = &a;
    p = 0;
    cout << (p == NULL )<< endl;
    return 0;
}
