unit usqldbvt;

{$mode delphi}{$H+}

interface

uses
  Classes, SysUtils, uSqlite3Helper, sqlite3dyn, usqlite3virtualTable, LCLProc,
  sqldb;

type

  { TSQLCursor }

  TSQLCursor = class(TSQLiteVirtualTableCursor)
  private
    FTab: TSQLite3VTab;
    FSearchRecs : array of TSearchRec;
    FEof: Boolean;
    FPath : string;
    FGoUp : Boolean;
    FType: String;
  public
    constructor Create(vTab : TSQLite3VTab);override;
    destructor Destroy; override;
    function SearchPath(aPath,aType : string) : Boolean;
    function Search(Prepared : TSQLVirtualTablePrepared) : Boolean;override;
    function Column(Index : Integer;var Res : TSQLVar) : Boolean;override;
    function Next : Boolean;override;
    function Eof : Boolean;override;
  end;

  TSQLCursorClass = class of TSQLCursor;

  { TSQLTable }

  TSQLTable = class(TSQLiteVirtualTable)
  private
    FConn: TSQLConnector;
  public
    function Prepare(var Prepared: TSQLVirtualTablePrepared): Boolean; override;
    function GetName: string; override;
    function CursorClass: TSQLiteVirtualTableCursorClass; override;
    {arguments:
      1. create table statement -> will ensure that table is there on remote database and have the right structure (fields are added when needed)
      1. select from staement -> will be able to make an read-only table from every possible select statement
      1. tablename -> greates an r/w table with same structure as it is in external database

      2. type
      3. host
      4. databasename
      5. username
      6. password
      7. properties
    }
    function GenerateStructure: string; override;
    destructor Destroy; override;
  end;

implementation

var
  fModule: TSQLite3Module;
  aMS : TMemoryStream;

{ TSQLTable }

function TSQLTable.Prepare(var Prepared: TSQLVirtualTablePrepared): Boolean;
var
  i: Integer;
  aPrep: PSQLVirtualTablePreparedConstraint;
begin
  for i := 0 to Prepared.WhereCount-1 do
    begin
      aPrep := @Prepared.Where[i];
      if (aPrep^.Column>=1) and (aPrep^.Column<2)
      and (
         (aPrep^.Operation = soLike)
      or (aPrep^.Operation = soEqualTo)
      or (aPrep^.Operation = soBeginWith)
      or (aPrep^.Operation = soContains)
      ) then //we can filter only in Name and Path all other Filters should be done by sqlite
        aPrep^.Value.VType := ftNull;
    end;
  Result := True;
end;

function TSQLTable.GetName: string;
begin
  Result := 'sqldb';
end;

function TSQLTable.CursorClass: TSQLiteVirtualTableCursorClass;
begin
  Result := TSQLCursor;
end;

function TSQLTable.GenerateStructure: string;
begin
  FConn := TSQLConnector.Create(Self);
  FConn.ConnectorType:=Arguments[1];
  FConn.HostName:=Arguments[2];
  FConn.DatabaseName:=Arguments[3];
  FConn.UserName:=Arguments[4];
  FConn.Password:=Arguments[5];
  FConn.Open;
  Result := 'CREATE TABLE SQLDB (';
  Result += ')';
end;

destructor TSQLTable.Destroy;
begin
  FConn.Close;
  inherited Destroy;
end;

function IncludeTrailingSlash(s : string) : string;
begin
  Result := stringreplace(IncludeTrailingPathDelimiter(s),DirectorySeparator,'/',[rfReplaceAll]);
end;

{ TSQLCursor }

constructor TSQLCursor.Create(vTab: TSQLite3VTab);
begin
  FTab := vTab;
  FGoUp:=False;
end;

destructor TSQLCursor.Destroy;
begin
  while length(FSearchRecs)>0 do
    begin
      FindClose(FSearchRecs[length(FSearchRecs)-1]);
      SetLength(FSearchRecs,length(FSearchRecs)-1);
    end;
  inherited Destroy;
end;

function TSQLCursor.SearchPath(aPath, aType: string): Boolean;
var
  FSr: TSearchRec;
begin
  FPath:=aPath;
  FEof := FindFirst(StringReplace(IncludeTrailingSlash(FPath),'/',DirectorySeparator,[rfReplaceAll])+aType, faAnyFile and faDirectory,FSr) <> 0;
  setlength(FSearchRecs,length(FSearchRecs)+1);
  FSearchRecs[Length(FSearchRecs)-1] := Fsr;
  if (not FEof) and (FSR.Name='.') then Result := Next;
end;

function TSQLCursor.Search(Prepared: TSQLVirtualTablePrepared): Boolean;
var
  i: Integer;
  aPrep: PSQLVirtualTablePreparedConstraint;
  tmp: PUTF8Char;
begin
  Result := True;
  {$ifdef Windows}
  FPath:='c:/';
  {$else}
  FPath:='/';
  {$endif}
  FType := '*';
  for i := 0 to Prepared.WhereCount-1 do
    begin
      aPrep := @Prepared.Where[i];
      if aPrep.Column = 1 then //path
        begin
          case aPrep.Operation of
          soEqualTo:FPath:=aPrep.Value.VText;
          soBeginWith:FPath:=aPrep.Value.VText+'*';
          soLike:FPath:=aPrep.Value.VText;
          end;
        end;
    end;
  FPath := StringReplace(FPath,'/%','/',[]);
  SearchPath(StringReplace(FPath,DirectorySeparator,'/',[rfReplaceAll]),FType);
end;

function TSQLCursor.Column(Index: Integer; var Res: TSQLVar): Boolean;
var
  aFile: String;
begin
  Res.VType:=ftNull;
  case Index of
  //-1:Res := Fsr.Time;
  0:begin
      Res.VType:=ftUTF8;
      Res.VText:= PUTF8Char(FSearchRecs[length(FSearchRecs)-1].Name);//name
    end;
  1:begin
      Res.VType:=ftUTF8;
      Res.VText:=PUTF8Char(FPath);//path
    end;
  2:begin
      Res.VType:=ftInt64;
      if FSearchRecs[length(FSearchRecs)-1].Attr and faDirectory = faDirectory then
        Res.VInt64:= 1
      else
        Res.VInt64:= 0; //isdir
    end;
  3:begin
      Res.VType:=ftInt64;
      Res.VInt64 := FSearchRecs[length(FSearchRecs)-1].Size;//size
    end;
  4:begin
      Res.VType:=ftDate;
      Res.VDateTime:=FileDateToDateTime(FSearchRecs[length(FSearchRecs)-1].Time); //mtime
    end;
  5:begin
      aFile := StringReplace(FPath+FSearchRecs[length(FSearchRecs)-1].Name,'/',DirectorySeparator,[rfReplaceAll]);
      if FileExists(aFile) and (not DirectoryExists(aFile)) then
        begin
          FreeAndNil(aMS);
          try
            aMS := TMemoryStream.Create;
            aMS.LoadFromFile(aFile);
            Res.VType:=ftBlob;
            Res.VBlob:=aMS.Memory;
            Res.VBlobLen := aMS.Size;
          except
            FreeAndNil(aMS);
          end;
        end;
    end;
  //ctime
  //atime
  end;
  Result := True;
end;

function TSQLCursor.Next: Boolean;
label retry;
begin
  Result := True;
retry:
  if (not Feof) and (FSearchRecs[length(FSearchRecs)-1].Attr and faDirectory = faDirectory) and( not ((FSearchRecs[length(FSearchRecs)-1].Name='.') or (FSearchRecs[length(FSearchRecs)-1].Name='..')))  then
    SearchPath(IncludeTrailingSlash(IncludeTrailingSlash(FPath)+FSearchRecs[length(FSearchRecs)-1].Name),FType);
  if FEof and (length(FSearchRecs)>0) then
    begin
      if pos('/',FPath)>0 then
        begin
          FPath := copy(FPath,0,LastDelimiter('/',FPath)-1);
          FPath := copy(FPath,0,LastDelimiter('/',FPath));
        end;
      FindClose(FSearchRecs[length(FSearchRecs)-1]);
      SetLength(FSearchRecs,length(FSearchRecs)-1);
      if length(FSearchRecs)=0 then
        begin
          FEof:=True;
          exit;
        end;
      feof := FindNext(FSearchRecs[length(FSearchRecs)-1]) <> 0;
    end
  else
    feof := FindNext(FSearchRecs[length(FSearchRecs)-1]) <> 0;
  if (not FEof) and ((FSearchRecs[length(FSearchRecs)-1].Name='.') or (FSearchRecs[length(FSearchRecs)-1].Name='..')) then goto retry;
  if FEof and (length(FSearchRecs)>0) then
    goto retry;
  if length(FSearchRecs)>0 then
    debugln(IntToStr(length(FSearchRecs))+':'+IncludeTrailingSlash(FPath)+FSearchRecs[length(FSearchRecs)-1].Name)
end;

function TSQLCursor.Eof: Boolean;
begin
  result := FEof;
end;

end.

