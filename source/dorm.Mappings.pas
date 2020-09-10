unit dorm.Mappings;

interface

uses
  Rtti,
  Generics.Collections,
  superobject;

type

{$RTTI EXPLICIT
  FIELDS([vcPrivate, vcProtected, vcPublic, vcPublished])
  METHODS([vcPrivate, vcProtected, vcPublic, vcPublished])
  PROPERTIES([vcPrivate, vcProtected, vcPublic, vcPublished])}
  // Mapping Attibutes
  Entity = class(TCustomAttribute)
  private
    FTableName: string;
    FPackage: string;
    FSaveHistory: boolean;

  public
    constructor Create(const ATableName: string = ''; const APackageName: string = ''; ASaveHistory : boolean = false);
    property TableName: string read FTableName;
    property package: string read FPackage;
    property SaveHistory : boolean read FSaveHistory;
  end;

  Column = class(TCustomAttribute)
  private
    FFieldName: string;
    FFieldType: string;
    FSize: Cardinal;
    FPrecision: Cardinal;
    FDefaultValue: string;

  public
    constructor Create; overload;
    constructor Create(const AColumnName: string; ASize: Cardinal = 0; APrecision: Cardinal = 0;
      ADefaultValue: string = ''); overload;
    constructor Create(const AColumnName: string; AFieldType: string; ASize: Cardinal = 0;
      APrecision: Cardinal = 0; ADefaultValue: string = ''); overload;
    property FieldName: string read FFieldName write FFieldName;
    property FieldType: string read FFieldType write FFieldType;
    property Size: Cardinal read FSize write FSize;
    property Precision: Cardinal read FPrecision write FPrecision;
    property DefaultValue: string read FDefaultValue write FDefaultValue;
  end;

  NoAutomapping = class(TCustomAttribute)
  end;

  { if this attribute is present and the value type is integer, float, date, datetime or time, and
    the value is 0, then in the database is inserted a null instead of 0 }
  Nullable = class(TCustomAttribute)
  end;

  Size = class(TCustomAttribute)
  private
    FColumnSize: Cardinal;
    FColumnPrecision: Cardinal;

  public
    constructor Create(const ASize: Cardinal; const APrecision: Cardinal = 0);
    property ColumnSize: Cardinal read FColumnSize;
    property ColumnPrecision: Cardinal read FColumnPrecision;
  end;

  DefaultValue = class(TCustomAttribute)
  private
    FValue: string;

  public
    constructor Create(const ADefaultValue: string);
    property Value: string read FValue;
  end;

  ListOf = class(TCustomAttribute)
  private
    FValue: string;

  public
    constructor Create(const ADefaultValue: string);
    property Value: string read FValue;
  end;

  TCustomRelationAttribute = class(TCustomAttribute)
  private
    FChildPropertyName: string;
    FLazyLoad: boolean;

  public
    constructor Create(const AChildPropertyName: string; ALazyLoad: boolean = False);
    property ChildPropertyName: string read FChildPropertyName;
    property LazyLoad: boolean read FLazyLoad;
  end;

  Lazy = class(TCustomAttribute);
  HasOne = class(TCustomRelationAttribute);
  HasMany = class(TCustomRelationAttribute);

  BelongsTo = class(TCustomAttribute)
  private
    FRefPropertyName: string;
    FFixRelation,
    FLazyLoad: boolean;
  public
    constructor Create(const ARefPropertyName: string; ALazyLoad: boolean = False; AFixRelation: boolean = False);
    property RefPropertyName: string read FRefPropertyName;
    property LazyLoad: boolean read FLazyLoad;
    property FixRelation:  boolean read FFixRelation;
  end;

  Id = class(TCustomAttribute)
  private
    FIsFK : boolean;
  public
    constructor Create(_isFK : boolean = false);
    property IsFK : boolean read FIsFK;
  end;
  Transient = class(TCustomAttribute);

  ValueFromFunction = class(TCustomAttribute)
  private
    FFuncName: string;
    FTriggerVal: TValue;
  public
    constructor Create(const _FuncName : string; _TriggerVal : integer); overload;
    constructor Create(const _FuncName : string; _TriggerVal : double); overload;
    constructor Create(const _FuncName : string; _TriggerVal : boolean); overload;
    constructor Create(const _FuncName : string; _TriggerVal : string); overload;
    property TriggerVal : TValue read FTriggerVal;
    property FuncName : string read FFuncName;
  end;

  Join = class(TCustomAttribute)
  private
    FJoinClause: string;
    FQualifier: string;
  public
    constructor Create(const _Qualifier : string; const _JoinClause : string = '');
    property Qualifier : string read FQualifier;
    property JoinClause : string read FJoinClause;
  end;

  // Mapping classes
  TdormIndexType = (itNone, itIndex, itUnique);
  TdormKeyType = (ktInteger, ktString);
  TMappingTable = class;
  TMappingField = class;

  TMappingCache = record
    RTTIField: TRttiField;
    RTTIProp: TRttiProperty;
  end;

  TMappingField = class
  private
    FPK: boolean;
    FName: string;
    FFieldType: string;
    FDefaultValue: string;
    FIndexType: TdormIndexType;
    FPrecision: Cardinal;
    FFieldName: string;
    FSize: Cardinal;
    FRTTICache: TMappingCache;
    FNullable: boolean;
    FIsFK: boolean;
    procedure SetNullable(const Value: boolean);
    procedure SetIsFK(const Value: boolean);

  public
    constructor Create;
    procedure Assign(Source: TMappingField);
    function ToString: string; override;
    property name: string read FName write FName;
    property FieldName: string read FFieldName write FFieldName;
    property FieldType: string read FFieldType write FFieldType;
    property DefaultValue: string read FDefaultValue write FDefaultValue;
    property Size: Cardinal read FSize write FSize;
    property Precision: Cardinal read FPrecision write FPrecision;
    property IndexType: TdormIndexType read FIndexType write FIndexType;
    property IsPK: boolean read FPK write FPK;
    property IsFK: boolean read FIsFK write SetIsFK;
    property RTTICache: TMappingCache read FRTTICache write FRTTICache;
    property Nullable: boolean read FNullable write SetNullable;
  end;

  TMappingRelation = class
  private
    FName: string;
    FChildClassName: string;
    FChildFieldName: string;
    FLazyLoad: boolean;
    FRTTICache: TMappingCache;

  public
    property name: string read FName write FName;
    property ChildClassName: string read FChildClassName write FChildClassName;
    property ChildFieldName: string read FChildFieldName write FChildFieldName;
    property LazyLoad: boolean read FLazyLoad write FLazyLoad;
    property RTTICache: TMappingCache read FRTTICache write FRTTICache;
    procedure Assign(Source: TMappingRelation);
  end;

  TMappingBelongsTo = class
  private
    FName: string;
    FOwnerClassName: string;
    FRefFieldName: string;
    FLazyLoad: boolean;
    FRTTICache: TMappingCache;
    FFixRelation: boolean;

  public
    property name: string read FName write FName;
    property OwnerClassName: string read FOwnerClassName write FOwnerClassName;
    property RefFieldName: string read FRefFieldName write FRefFieldName;
    property LazyLoad: boolean read FLazyLoad write FLazyLoad;
    property FixRelation: boolean read FFixRelation write FFixRelation;
    property RTTICache: TMappingCache read FRTTICache write FRTTICache;
    procedure Assign(Source: TMappingBelongsTo);
  end;

  TJoinTable = class
  private
    FJoinClause: string;
    FPropName: string;
    FQualifier: string;
    FMappingTable: TMappingTable;
  public
    property PropName : string read FPropName write FPropName;
    property Qualifier: string read FQualifier write FQualifier;
    property JoinClause: string read FJoinClause write FJoinClause;
    property MappingTable: TMappingTable read FMappingTable write FMappingTable;
    procedure Assign(Source: TJoinTable);
  end;

  TMappingFieldList = class(TObjectList<TMappingField>)
  public
    constructor Create;
    function BinSearchByFieldName(const _Fieldname : string): TMappingField;
  end;
  TMappingRelationList = class(TObjectList<TMappingRelation>);
  TMappingBelongsToList = class(TObjectList<TMappingBelongsTo>);
  TJoinTableList = class(TObjectList<TJoinTable>);

  TMappingTable = class
  private
    FPackage: string;
    FTableName: string;
    FFields: TMappingFieldList;
    FBelongsToList: TMappingBelongsToList;
    FHasManyList: TMappingRelationList;
    FHasOneList: TMappingRelationList;
    FJoinTableList : TJoinTableList;
    FSaveHistory: boolean;
    function GetId: TMappingField;
    procedure SetSaveHistory(const Value: boolean);

  public
    constructor Create;
    destructor Destroy; override;
    function FindByName(const AName: string): TMappingField;
    function FindHasOneByName(const AName: string): TMappingRelation;
    function FindHasManyByName(const AName: string): TMappingRelation;
    function FindBelongsToByName(const AName: string): TMappingBelongsTo;
    function FindJoinTableByName(const AName: string): TJoinTable;
    function AddField: TMappingField;
    function AddBelongsTo: TMappingBelongsTo;
    function AddHasMany: TMappingRelation;
    function AddHasOne: TMappingRelation;
    function AddJoinTable: TJoinTable;
    function ToString: string; override;
    property package: string read FPackage write FPackage;
    property TableName: string read FTableName write FTableName;
    property SaveHistory : boolean read FSaveHistory write SetSaveHistory;
    property Id: TMappingField read GetId;
    property Fields: TMappingFieldList read FFields;
    property HasManyList: TMappingRelationList read FHasManyList;
    property HasOneList: TMappingRelationList read FHasOneList;
    property BelongsToList: TMappingBelongsToList read FBelongsToList;
    property JoinTableList: TJoinTableList read FJoinTableList;
  end;

implementation

uses
  SysUtils,
  Variants,
  StrUtils,
  dorm.Utils,
  dorm.Commons, System.Generics.Defaults;
{ Entity }

constructor Entity.Create(const ATableName: string = ''; const APackageName: string = ''; ASaveHistory : boolean = false);
begin
  FTableName := ATableName;
  FPackage := APackageName;
  FSaveHistory := ASaveHistory;
end;
{ Column }

constructor Column.Create;
begin
  inherited Create;
  FFieldName := EmptyStr; // must be EmptyStr
  FFieldType := EmptyStr; // must be EmptyStr
  FDefaultValue := EmptyStr; // must be EmptyStr
  FSize := 0;
  FPrecision := 0;
end;

constructor Column.Create(const AColumnName: string; ASize: Cardinal = 0; APrecision: Cardinal = 0;
  ADefaultValue: string = '');
begin
  Create;
  FSize := ASize;
  FFieldName := AColumnName;
  FPrecision := APrecision;
  FDefaultValue := ADefaultValue;
end;

constructor Column.Create(const AColumnName: string; AFieldType: string; ASize: Cardinal = 0;
  APrecision: Cardinal = 0; ADefaultValue: string = '');
begin
  Create(AColumnName, ASize, APrecision, ADefaultValue);
  FFieldType := LowerCase(AFieldType);
end;
{ Size }

constructor Size.Create(const ASize: Cardinal; const APrecision: Cardinal = 0);
begin
  inherited Create;
  FColumnSize := ASize;
  FColumnPrecision := APrecision;
end;
{ DefaultValue }

constructor DefaultValue.Create(const ADefaultValue: string);
begin
  inherited Create;
  FValue := ADefaultValue;
end;
{ TCustomRelationAttribute }

constructor TCustomRelationAttribute.Create(const AChildPropertyName: string;
  ALazyLoad: boolean = False);
begin
  inherited Create;
  FChildPropertyName := AChildPropertyName;
  FLazyLoad := ALazyLoad;
end;
{ BelongsTo }

constructor BelongsTo.Create(const ARefPropertyName: string; ALazyLoad: boolean = False; AFixRelation: boolean = False);
begin
  inherited Create;
  FRefPropertyName := ARefPropertyName;
  FLazyLoad := ALazyLoad;
  FFixRelation := AFixRelation;
end;
{ TMappingTable }

constructor TMappingTable.Create;
begin
  FFields := TMappingFieldList.Create;
  FHasOneList := TMappingRelationList.Create;
  FHasManyList := TMappingRelationList.Create;
  FBelongsToList := TMappingBelongsToList.Create;
  FJoinTableList := TJoinTableList.Create;
  FTableName := '';
  FPackage := '';
end;

destructor TMappingTable.Destroy;
begin
  FFields.Free;
  FHasManyList.Free;
  FHasOneList.Free;
  FBelongsToList.Free;
  FJoinTableList.Free;
  inherited;
end;

function TMappingTable.FindBelongsToByName(const AName: string): TMappingBelongsTo;
var
  _Rel: TMappingBelongsTo;
begin
  Result := nil;
  for _Rel in BelongsToList do
    if AnsiSameText(_Rel.Name, AName) then
      Exit(_Rel);
end;

function TMappingTable.FindByName(const AName: string): TMappingField;
var
  _Field: TMappingField;
begin
  Result := nil;
  for _Field in Fields do
    if AnsiSameText(_Field.Name, AName) then
      Exit(_Field);
end;

// function TMappingTable.GetFieldByAttribute(const AAttributeName: string): TMappingField;
// var
// fm: TMappingField;
// begin
// for fm in Fields do
// if fm.Name = AAttributeName then
// Exit(fm);
// raise EdormException.CreateFmt('Unknown field attribute %s', [AAttributeName]);
// end;

function TMappingTable.FindHasManyByName(const AName: string): TMappingRelation;
var
  _Rel: TMappingRelation;
begin
  Result := nil;
  for _Rel in HasManyList do
    if AnsiSameText(_Rel.Name, AName) then
      Exit(_Rel);
end;

function TMappingTable.FindHasOneByName(const AName: string): TMappingRelation;
var
  _Rel: TMappingRelation;
begin
  Result := nil;
  for _Rel in HasOneList do
    if AnsiSameText(_Rel.Name, AName) then
      Exit(_Rel);
end;

function TMappingTable.FindJoinTableByName(const AName: string): TJoinTable;
var
  _Rel: TJoinTable;
begin
  Result := nil;
  for _Rel in JoinTableList do
    if AnsiSameText(_Rel.PropName, AName) then
      Exit(_Rel);
end;

function TMappingTable.GetId: TMappingField;
var
  FixRelation: TMappingField;
begin
  Result := nil;
  for FixRelation in Fields do
    if FixRelation.IsPK then
      Exit(FixRelation);
end;

procedure TMappingTable.SetSaveHistory(const Value: boolean);
begin
  FSaveHistory := Value;
end;

function TMappingTable.AddBelongsTo: TMappingBelongsTo;
begin
  Result := TMappingBelongsTo.Create;
  FBelongsToList.Add(Result);
end;

function TMappingTable.AddField: TMappingField;
begin
  Result := TMappingField.Create;
  FFields.Add(Result);
end;

function TMappingTable.AddHasMany: TMappingRelation;
begin
  Result := TMappingRelation.Create;
  FHasManyList.Add(Result);
end;

function TMappingTable.AddHasOne: TMappingRelation;
begin
  Result := TMappingRelation.Create;
  FHasOneList.Add(Result);
end;

function TMappingTable.AddJoinTable: TJoinTable;
begin
  Result := TJoinTable.Create;
  FJoinTableList.Add(Result);
end;

function TMappingTable.ToString: string;
begin
  Result := Format('Package: %s,  Table: %s', [package, TableName]);
end;
{ TMappingField }

procedure TMappingField.Assign(Source: TMappingField);
begin
  FPK := Source.IsPK;
  FName := Source.Name;
  FNullable := Source.Nullable;
  FFieldType := Source.FieldType;
  FDefaultValue := Source.DefaultValue;
  FIndexType := Source.IndexType;
  FPrecision := Source.Precision;
  FFieldName := Source.FieldName;
  FSize := Source.Size;
  FRTTICache := Source.RTTICache;
  FIsFK := Source.IsFK;
end;

constructor TMappingField.Create;
begin
  inherited;
  FName := '';
  FFieldName := '';
  FFieldType := '';
  FDefaultValue := '';
  FSize := 0;
  FPrecision := 0;
  FIndexType := itNone;
  FNullable := False;
  FRTTICache.RTTIField := nil;
  FRTTICache.RTTIProp := nil;
  FIsFK := False;
end;

procedure TMappingField.SetIsFK(const Value: boolean);
begin
  FIsFK := Value;
end;

procedure TMappingField.SetNullable(const Value: boolean);
begin
  FNullable := Value;
end;

function TMappingField.ToString: string;
begin
  Result := Format('PK: %s, name: %s, field: %s, field type: %s',
    [BoolToStr(IsPK, True), name, FieldName, FieldType]);
end;
{ TMappingRelation }

procedure TMappingRelation.Assign(Source: TMappingRelation);
begin
  FName := Source.Name;
  FChildClassName := Source.ChildClassName;
  FChildFieldName := Source.ChildFieldName;
  FLazyLoad := Source.LazyLoad;
  FRTTICache := Source.RTTICache;
end;
{ TMappingBelongsTo }

procedure TMappingBelongsTo.Assign(Source: TMappingBelongsTo);
begin
  FName := Source.Name;
  FOwnerClassName := Source.OwnerClassName;
  FRefFieldName := Source.RefFieldName;
  FLazyLoad := Source.LazyLoad;
  FFixRelation := Source.FixRelation;
  FRTTICache := Source.RTTICache;
end;

{ ListOf }

constructor ListOf.Create(const ADefaultValue: string);
begin
  inherited Create;
  FValue := ADefaultValue;
end;

{ Id }

constructor Id.Create(_isFK: boolean);
begin
  inherited Create;
  FisFK := _isFK;
end;

{ ValueFromFunction }

constructor ValueFromFunction.Create(const _FuncName : string; _TriggerVal : integer);
begin
  FTriggerVal := _TriggerVal;
  FFuncName := _FuncName;
end;

constructor ValueFromFunction.Create(const _FuncName: string;
  _TriggerVal: double);
begin
  FTriggerVal := _TriggerVal;
  FFuncName := _FuncName;
end;

constructor ValueFromFunction.Create(const _FuncName: string;
  _TriggerVal: boolean);
begin
  FTriggerVal := _TriggerVal;
  FFuncName := _FuncName;
end;

constructor ValueFromFunction.Create(const _FuncName: string;
  _TriggerVal: string);
begin
  FTriggerVal := _TriggerVal;
  FFuncName := _FuncName;
end;

{ Join }

constructor Join.Create(const _Qualifier, _JoinClause: string);
begin
  FQualifier := _Qualifier;
  FJoinClause := _JoinClause;
end;

{ TJoinTable }

procedure TJoinTable.Assign(Source: TJoinTable);
begin
  FPropName := Source.PropName;
  FQualifier := Source.Qualifier;
  FJoinClause := Source.JoinClause;
  FMappingTable := Source.MappingTable;
end;

{ TMappingFieldList }

function TMappingFieldList.BinSearchByFieldName(
  const _Fieldname: string): TMappingField;
var ToFind: TMappingField;
    Idx: Integer;
begin
  Result := nil;
  if Count > 0 then begin
    ToFind := TMappingField.Create;
    try
      ToFind.FieldName := _Fieldname;
      if BinarySearch(ToFind, Idx) then begin
        Result := Items[Idx];
      end;
    finally
      ToFind.DisposeOf;
    end;
  end;end;

constructor TMappingFieldList.Create;
begin
  inherited Create(TComparer<TMappingField>.Construct(function (const L, R: TMappingField): Integer
                    begin
                      Result:= CompareText(L.FieldName, R.FieldName);
                    end
                  ));
end;

end.
