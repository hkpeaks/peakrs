# PyPeaks Is Under Construction
PyPeaks is an ETL Framework using Arrow Compatible Python Libraries as Calc Engines

## Development of New SQL Statement for File, In-memory Table and Network Stream

#### UserDefineFunctionName: Extraction ~ Load

| Transformation

#### UserDefineFunctionName: SourceFile/Table ~ ResultFile/Table

| Command: Setting

#### ExpandFile: Fact.csv ~ 1BillionRows.csv

| ExpandFactor: 123

#### JoinScenario1: 1BillionRows.csv ~ Test1Results.csv

| JoinTable: Quantity, Unit_Price => InnerJoin(Master)Multiply(Amount)

| OrderBy: Date(D) => CreateFolderLake(Shop)

| Select: Date,Shop,Style,Product,Quantity,Amount

#### BuildKeyValueTable: Master.csv ~ KeyValueTable

| BuildKeyValue: Product, Style

#### JoinScenario2: 1BillionRows.csv ~ Test2AResults.csv

| JoinKeyValue: Product, Style => AllMatch(KeyValueTable)

| AddColumn: Quantity, Unit_Price => Multiply(Amount)

| Filter: Amount(Float > 50000)

| GroupBy: Product, Style => Count() Sum(Quantity) Sum(Amount)

| OrderBy: Shop(A)Product(A)Date(D)

#### SplitFile: Test1Results.csv ~ FolderLake

| CreateFolderLake: Shop

#### FilterFolder: Outbox/FolderLake/S15/*.csv ~ Result-FilterFolderLake.csv

| Filter: Product(222..888) Style(=F)

#### ReadSample2View: Outbox/Result-FilterFolderLake.csv ~ SampleTable

| ReadSample: StartPosition%(0) ByteLength(100000)

| View

