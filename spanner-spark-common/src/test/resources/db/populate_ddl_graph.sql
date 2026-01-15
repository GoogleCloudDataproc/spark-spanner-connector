-- FlexibleGraph

CREATE TABLE   FlexibleGraphNode
(
    id         INT64 NOT NULL,
    type       STRING( MAX) NOT NULL,
    properties JSON,
    cluster_id INT64,
) PRIMARY KEY(id);

CREATE INDEX NodeByType ON FlexibleGraphNode (type);

CREATE TABLE FlexibleGraphEdge
(
    id         INT64 NOT NULL,
    edge_type  STRING( MAX) NOT NULL,
    to_id      INT64 NOT NULL,
    edge_id    STRING( MAX),
    properties JSON,
    CONSTRAINT FK_ToNode FOREIGN KEY (to_id) REFERENCES   FlexibleGraphNode (id),
) PRIMARY KEY(id, edge_type, to_id, edge_id),
  INTERLEAVE IN PARENT   FlexibleGraphNode ON DELETE CASCADE;

CREATE PROPERTY GRAPH FlexibleGraph
  NODE TABLES(
      FlexibleGraphNode
      KEY(id)
      LABEL Node PROPERTIES(
        cluster_id,
        id,
        properties,
        type)
  )
  EDGE TABLES(
    FlexibleGraphEdge
      KEY(id, edge_type, to_id, edge_id)
      SOURCE KEY(id) REFERENCES   FlexibleGraphNode(id)
      DESTINATION KEY(to_id) REFERENCES   FlexibleGraphNode(id)
      LABEL Edge PROPERTIES(
        edge_id,
        edge_type,
        id,
        properties,
        to_id)
  );

-- MusicGraph

CREATE TABLE ProductionCompanies
(
    CompanyId       INT64 NOT NULL,
    CompanyName     STRING( MAX) NOT NULL,
    LocationCountry STRING( MAX) NOT NULL,
    FoundedYear     INT64 NOT NULL,
) PRIMARY KEY(CompanyId);

CREATE TABLE Singers
(
    SingerId  INT64 NOT NULL,
    FirstName STRING(1024),
    LastName  STRING(1024),
    BirthDate DATE,
) PRIMARY KEY(SingerId);

CREATE TABLE Albums
(
    SingerId    INT64 NOT NULL,
    AlbumId     INT64 NOT NULL,
    AlbumTitle  STRING( MAX),
    ReleaseDate DATE,
    CompanyId   INT64 NOT NULL,
    CONSTRAINT FKProductionCompanyId FOREIGN KEY (CompanyId) REFERENCES ProductionCompanies (CompanyId),
) PRIMARY KEY(SingerId, AlbumId),
  INTERLEAVE IN PARENT Singers ON
DELETE
CASCADE;

CREATE TABLE SingerContracts
(
    SingerId  INT64 NOT NULL,
    CompanyId INT64 NOT NULL,
    CONSTRAINT FKSingerCompanyId FOREIGN KEY (CompanyId) REFERENCES ProductionCompanies (CompanyId),
) PRIMARY KEY(SingerId, CompanyId),
  INTERLEAVE IN PARENT Singers ON
DELETE
CASCADE;

CREATE TABLE SingerFriends
(
    SingerId INT64 NOT NULL,
    FriendId INT64 NOT NULL,
    CONSTRAINT FKSingerFriendId FOREIGN KEY (FriendId) REFERENCES Singers (SingerId),
) PRIMARY KEY(SingerId, FriendId),
  INTERLEAVE IN PARENT Singers ON
DELETE
CASCADE;

CREATE OR REPLACE PROPERTY GRAPH MusicGraph
  NODE TABLES(
    Albums AS Album
      KEY(SingerId, AlbumId)
      LABEL ALBUM PROPERTIES(
        AlbumId,
        AlbumTitle,
        CompanyId,
        ReleaseDate,
        SingerId),

    ProductionCompanies AS Company
      KEY(CompanyId)
      LABEL MUSIC_COMPANY PROPERTIES(
        FoundedYear AS founded_year,
        CompanyName AS name)
      LABEL MUSIC_CREATOR PROPERTIES(
        LocationCountry AS country_origin,
        CompanyName AS name),

    Singers AS Singer
      KEY(SingerId)
      LABEL MUSIC_CREATOR PROPERTIES(
        "US" AS country_origin,
        CONCAT(FirstName, " ", LastName) AS name)
      LABEL SINGER PROPERTIES(
        BirthDate AS birthday,
        SingerId AS id,
        CONCAT(FirstName, " ", LastName) AS singer_name)
  )
  EDGE TABLES(
    Albums AS COMPANY_PRODUCES_ALBUM
      KEY(CompanyId, SingerId, AlbumId)
      SOURCE KEY(CompanyId) REFERENCES Company(CompanyId)
      DESTINATION KEY(AlbumId, SingerId) REFERENCES Album(AlbumId, SingerId)
      LABEL CREATES_MUSIC PROPERTIES(
        AlbumId AS album_id,
        ReleaseDate AS release_date),

    Albums AS SINGER_CREATES_ALBUM
      KEY(SingerId, AlbumId)
      SOURCE KEY(SingerId) REFERENCES Singer(SingerId)
      DESTINATION KEY(AlbumId, SingerId) REFERENCES Album(AlbumId, SingerId)
      LABEL CREATES_MUSIC PROPERTIES(
        AlbumId AS album_id,
        ReleaseDate AS release_date),

    SingerFriends AS SINGER_HAS_FRIEND
      KEY(SingerId, FriendId)
      SOURCE KEY(SingerId) REFERENCES Singer(SingerId)
      DESTINATION KEY(FriendId) REFERENCES Singer(SingerId)
      LABEL KNOWS PROPERTIES(
        FriendId,
        SingerId),

    SingerContracts AS SINGER_SIGNED_BY_COMPANY
      KEY(SingerId, CompanyId)
      SOURCE KEY(SingerId) REFERENCES Singer(SingerId)
      DESTINATION KEY(CompanyId) REFERENCES Company(CompanyId)
      LABEL SIGNED_BY PROPERTIES(
        CompanyId,
        SingerId)
  );
