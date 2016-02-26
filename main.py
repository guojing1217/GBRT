from James_Gordan import James

if __name__ == "__main__":
    j = James()
    print '1 ' , j.name
    j.name = "James Gordon"
    print '3 ' , j.name
    print '5 ' , j.__dict__
