DROP TABLE IF EXISTS omopcdm.drug_strength CASCADE;
DROP TABLE IF EXISTS omopcdm.concept CASCADE;
DROP TABLE IF EXISTS omopcdm.concept_relationship CASCADE;
DROP TABLE IF EXISTS omopcdm.concept_ancestor CASCADE;
DROP TABLE IF EXISTS omopcdm.concept_synonym CASCADE;
DROP TABLE IF EXISTS omopcdm.relationship CASCADE;
DROP TABLE IF EXISTS omopcdm.concept_class CASCADE;
DROP TABLE IF EXISTS omopcdm.domain CASCADE;
DROP TABLE IF EXISTS omopcdm.vocabulary CASCADE;
DROP TABLE IF EXISTS omopcdm.source_to_concept_map CASCADE;

CREATE TABLE IF NOT EXISTS omopcdm.CONCEPT
(

    concept_id       integer       NOT NULL,
    concept_name     varchar(255) NOT NULL,
    domain_id        varchar(30)  NOT NULL,
    vocabulary_id    varchar(30)  NOT NULL,
    concept_class_id varchar(30)  NOT NULL,
    standard_concept varchar(1)   NULL,
    concept_code     varchar(50)  NOT NULL,
    valid_start_date date         NOT NULL,
    valid_end_date   date         NOT NULL,
    invalid_reason   varchar(1)   NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.VOCABULARY
(

    vocabulary_id         varchar(30)  NOT NULL,
    vocabulary_name       varchar(255) NOT NULL,
    vocabulary_reference  varchar(255) NOT NULL,
    vocabulary_version    varchar(255) NULL,
    vocabulary_concept_id integer       NOT NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.DOMAIN
(

    domain_id         varchar(30)  NOT NULL,
    domain_name       varchar(255) NOT NULL,
    domain_concept_id integer       NOT NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.CONCEPT_CLASS
(

    concept_class_id         varchar(30)  NOT NULL,
    concept_class_name       varchar(255) NOT NULL,
    concept_class_concept_id integer       NOT NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.CONCEPT_RELATIONSHIP
(

    concept_id_1     integer     NOT NULL,
    concept_id_2     integer     NOT NULL,
    relationship_id  varchar(30) NOT NULL,
    valid_start_date date        NOT NULL,
    valid_end_date   date        NOT NULL,
    invalid_reason   varchar(1)  NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.RELATIONSHIP
(

    relationship_id         varchar(30)  NOT NULL,
    relationship_name       varchar(255) NOT NULL,
    is_hierarchical         varchar(1)   NOT NULL,
    defines_ancestry        varchar(1)   NOT NULL,
    reverse_relationship_id varchar(30)  NOT NULL,
    relationship_concept_id integer      NOT NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.CONCEPT_SYNONYM
(

    concept_id           integer       NOT NULL,
    concept_synonym_name varchar(1000) NOT NULL,
    language_concept_id  integer       NOT NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.CONCEPT_ANCESTOR
(

    ancestor_concept_id      integer NOT NULL,
    descendant_concept_id    integer NOT NULL,
    min_levels_of_separation integer NOT NULL,
    max_levels_of_separation integer NOT NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.SOURCE_TO_CONCEPT_MAP
(

    source_code             varchar(50)  NOT NULL,
    source_concept_id       integer       NOT NULL,
    source_vocabulary_id    varchar(30)  NOT NULL,
    source_code_description varchar(255) NULL,
    target_concept_id       integer       NOT NULL,
    target_vocabulary_id    varchar(30)  NOT NULL,
    valid_start_date        date         NOT NULL,
    valid_end_date          date         NOT NULL,
    invalid_reason          varchar(1)   NULL
);

--HINT DISTRIBUTE ON RANDOM
CREATE TABLE IF NOT EXISTS omopcdm.DRUG_STRENGTH
(

    drug_concept_id             integer    NOT NULL,
    ingredient_concept_id       integer    NOT NULL,
    amount_value                NUMERIC    NULL,
    amount_unit_concept_id      integer    NULL,
    numerator_value             NUMERIC    NULL,
    numerator_unit_concept_id   integer    NULL,
    denominator_value           NUMERIC    NULL,
    denominator_unit_concept_id integer    NULL,
    box_size                    integer    NULL,
    valid_start_date            date       NOT NULL,
    valid_end_date              date       NOT NULL,
    invalid_reason              varchar(1) NULL
);

CREATE OR REPLACE FUNCTION copyif(tablename text, filename text) RETURNS VOID AS
$func$
BEGIN
EXECUTE (
  format('DO
  $do$
  BEGIN
  IF NOT EXISTS (SELECT FROM %s) THEN
     COPY %s FROM ''%s'' WITH DELIMITER E''\t'' CSV HEADER QUOTE E''\b'' ;
  END IF;
  END
  $do$
', tablename, tablename, filename));
END
$func$ LANGUAGE plpgsql;

SELECT copyif('omopcdm.drug_strength', '/vocab/DRUG_STRENGTH.csv');
SELECT copyif('omopcdm.concept', '/vocab/CONCEPT.csv');
SELECT copyif('omopcdm.concept_relationship', '/vocab/CONCEPT_RELATIONSHIP.csv');
SELECT copyif('omopcdm.concept_ancestor', '/vocab/CONCEPT_ANCESTOR.csv');
SELECT copyif('omopcdm.concept_synonym', '/vocab/CONCEPT_SYNONYM.csv');
SELECT copyif('omopcdm.relationship', '/vocab/RELATIONSHIP.csv');
SELECT copyif('omopcdm.concept_class', '/vocab/CONCEPT_CLASS.csv');
SELECT copyif('omopcdm.domain', '/vocab/DOMAIN.csv');
SELECT copyif('omopcdm.vocabulary', '/vocab/VOCABULARY.csv');
-- SELECT copyif('omopcdm.source_to_concept_map', '/vocab/SOURCE_TO_CONCEPT_MAP.csv');

-- primary keys
ALTER TABLE omopcdm.concept
    ADD CONSTRAINT xpk_concept PRIMARY KEY (concept_id);
ALTER TABLE omopcdm.vocabulary
    ADD CONSTRAINT xpk_vocabulary PRIMARY KEY (vocabulary_id);
ALTER TABLE omopcdm.domain
    ADD CONSTRAINT xpk_domain PRIMARY KEY (domain_id);
ALTER TABLE omopcdm.concept_class
    ADD CONSTRAINT xpk_concept_class PRIMARY KEY (concept_class_id);
ALTER TABLE omopcdm.concept_relationship
    ADD CONSTRAINT xpk_concept_relationship PRIMARY KEY (concept_id_1, concept_id_2, relationship_id);
ALTER TABLE omopcdm.relationship
    ADD CONSTRAINT xpk_relationship PRIMARY KEY (relationship_id);
ALTER TABLE omopcdm.concept_ancestor
    ADD CONSTRAINT xpk_concept_ancestor PRIMARY KEY (ancestor_concept_id, descendant_concept_id);
ALTER TABLE omopcdm.source_to_concept_map
    ADD CONSTRAINT xpk_source_to_concept_map PRIMARY KEY (source_vocabulary_id, target_concept_id, source_code, valid_end_date);
ALTER TABLE omopcdm.drug_strength
    ADD CONSTRAINT xpk_drug_strength PRIMARY KEY (drug_concept_id, ingredient_concept_id);

-- constraints
CREATE UNIQUE INDEX idx_concept_concept_id ON omopcdm.concept (concept_id ASC);
CLUSTER omopcdm.concept USING idx_concept_concept_id;
CREATE INDEX idx_concept_code ON omopcdm.concept (concept_code ASC);
CREATE INDEX idx_concept_vocabluary_id ON omopcdm.concept (vocabulary_id ASC);
CREATE INDEX idx_concept_domain_id ON omopcdm.concept (domain_id ASC);
CREATE INDEX idx_concept_class_id ON omopcdm.concept (concept_class_id ASC);
CREATE INDEX idx_concept_id_varchar ON omopcdm.concept (CAST(concept_id AS VARCHAR));
CREATE UNIQUE INDEX idx_vocabulary_vocabulary_id ON omopcdm.vocabulary (vocabulary_id ASC);
CLUSTER omopcdm.vocabulary USING idx_vocabulary_vocabulary_id;
CREATE UNIQUE INDEX idx_domain_domain_id ON omopcdm.domain (domain_id ASC);
CLUSTER omopcdm.domain USING idx_domain_domain_id;
CREATE UNIQUE INDEX idx_concept_class_class_id ON omopcdm.concept_class (concept_class_id ASC);
CLUSTER omopcdm.concept_class USING idx_concept_class_class_id;
CREATE INDEX idx_concept_relationship_id_1 ON omopcdm.concept_relationship (concept_id_1 ASC);
CREATE INDEX idx_concept_relationship_id_2 ON omopcdm.concept_relationship (concept_id_2 ASC);
CREATE INDEX idx_concept_relationship_id_3 ON omopcdm.concept_relationship (relationship_id ASC);
CREATE UNIQUE INDEX idx_relationship_rel_id ON omopcdm.relationship (relationship_id ASC);
CLUSTER omopcdm.relationship USING idx_relationship_rel_id;
CREATE INDEX idx_concept_synonym_id ON omopcdm.concept_synonym (concept_id ASC);
CLUSTER omopcdm.concept_synonym USING idx_concept_synonym_id;
CREATE INDEX idx_concept_ancestor_id_1 ON omopcdm.concept_ancestor (ancestor_concept_id ASC);
CLUSTER omopcdm.concept_ancestor USING idx_concept_ancestor_id_1;
CREATE INDEX idx_concept_ancestor_id_2 ON omopcdm.concept_ancestor (descendant_concept_id ASC);
CREATE INDEX idx_source_to_concept_map_id_3 ON omopcdm.source_to_concept_map (target_concept_id ASC);
CLUSTER omopcdm.source_to_concept_map USING idx_source_to_concept_map_id_3;
CREATE INDEX idx_source_to_concept_map_id_1 ON omopcdm.source_to_concept_map (source_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_id_2 ON omopcdm.source_to_concept_map (target_vocabulary_id ASC);
CREATE INDEX idx_source_to_concept_map_code ON omopcdm.source_to_concept_map (source_code ASC);
CREATE INDEX idx_drug_strength_id_1 ON omopcdm.drug_strength (drug_concept_id ASC);
CLUSTER omopcdm.drug_strength USING idx_drug_strength_id_1;
CREATE INDEX idx_drug_strength_id_2 ON omopcdm.drug_strength (ingredient_concept_id ASC);

-- foreign key constraints
ALTER TABLE omopcdm.concept
    ADD CONSTRAINT fpk_concept_domain FOREIGN KEY (domain_id) REFERENCES omopcdm.domain (domain_id);
ALTER TABLE omopcdm.concept
    ADD CONSTRAINT fpk_concept_class FOREIGN KEY (concept_class_id) REFERENCES omopcdm.concept_class (concept_class_id);
ALTER TABLE omopcdm.concept
    ADD CONSTRAINT fpk_concept_vocabulary FOREIGN KEY (vocabulary_id) REFERENCES omopcdm.vocabulary (vocabulary_id);
ALTER TABLE omopcdm.vocabulary
    ADD CONSTRAINT fpk_vocabulary_concept FOREIGN KEY (vocabulary_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.domain
    ADD CONSTRAINT fpk_domain_concept FOREIGN KEY (domain_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_class
    ADD CONSTRAINT fpk_concept_class_concept FOREIGN KEY (concept_class_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_relationship
    ADD CONSTRAINT fpk_concept_relationship_c_1 FOREIGN KEY (concept_id_1) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_relationship
    ADD CONSTRAINT fpk_concept_relationship_c_2 FOREIGN KEY (concept_id_2) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_relationship
    ADD CONSTRAINT fpk_concept_relationship_id FOREIGN KEY (relationship_id) REFERENCES omopcdm.relationship (relationship_id);
ALTER TABLE omopcdm.relationship
    ADD CONSTRAINT fpk_relationship_concept FOREIGN KEY (relationship_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.relationship
    ADD CONSTRAINT fpk_relationship_reverse FOREIGN KEY (reverse_relationship_id) REFERENCES omopcdm.relationship (relationship_id);
ALTER TABLE omopcdm.concept_synonym
    ADD CONSTRAINT fpk_concept_synonym_concept FOREIGN KEY (concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_synonym
    ADD CONSTRAINT fpk_concept_synonym_language_concept FOREIGN KEY (language_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_ancestor
    ADD CONSTRAINT fpk_concept_ancestor_concept_1 FOREIGN KEY (ancestor_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.concept_ancestor
    ADD CONSTRAINT fpk_concept_ancestor_concept_2 FOREIGN KEY (descendant_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.source_to_concept_map
    ADD CONSTRAINT fpk_source_to_concept_map_v_1 FOREIGN KEY (source_vocabulary_id) REFERENCES omopcdm.vocabulary (vocabulary_id);
ALTER TABLE omopcdm.source_to_concept_map
    ADD CONSTRAINT fpk_source_to_concept_map_v_2 FOREIGN KEY (target_vocabulary_id) REFERENCES omopcdm.vocabulary (vocabulary_id);
ALTER TABLE omopcdm.source_to_concept_map
    ADD CONSTRAINT fpk_source_to_concept_map_c_1 FOREIGN KEY (target_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.drug_strength
    ADD CONSTRAINT fpk_drug_strength_concept_1 FOREIGN KEY (drug_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.drug_strength
    ADD CONSTRAINT fpk_drug_strength_concept_2 FOREIGN KEY (ingredient_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.drug_strength
    ADD CONSTRAINT fpk_drug_strength_unit_1 FOREIGN KEY (amount_unit_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.drug_strength
    ADD CONSTRAINT fpk_drug_strength_unit_2 FOREIGN KEY (numerator_unit_concept_id) REFERENCES omopcdm.concept (concept_id);
ALTER TABLE omopcdm.drug_strength
    ADD CONSTRAINT fpk_drug_strength_unit_3 FOREIGN KEY (denominator_unit_concept_id) REFERENCES omopcdm.concept (concept_id);
