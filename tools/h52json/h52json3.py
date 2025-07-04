# tables包走不通，换h5py试试
# 解析单个小文件没问题

import h5py

# input_file = "D:\coding\github\scu-2025JD-team05\.local\playground\msd_summary_file.h5"
input_file = ".local\playground\TRAAAAW128F429D538.h5"

# 打开HDF5文件
with h5py.File(input_file, 'r') as hdf:
    # 查看文件中的数据集
    print("Data sets in the file:", list(hdf.keys()))
    
    # 选择需要转换的数据集
    for set in list(hdf.keys()):
        dataset = hdf[set]  # 替换为你的数据集名称
        print(dataset)
        # data = dataset[:]  # 读取数据
        # print(data)

