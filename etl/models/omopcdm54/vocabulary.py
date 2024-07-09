"""vocabulary models for OMOPCDM"""

# pylint: disable=invalid-name
from typing import Any, Final

from ...util.db import get_environment_variable as get_schema_name
from ...util.freeze import freeze_instance
from ..modelutils import (
    FK,
    CharField,
    Column,
    DateField,
    IntField,
    NumericField,
    PKCharField,
    PKIdMixin,
    PKIntField,
    make_model_base,
)

VOCAB_SCHEMA: Final[str] = get_schema_name("VOCAB_SCHEMA", "vocab")
VocabularyModelBase: Any = make_model_base(schema=VOCAB_SCHEMA)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class Concept(VocabularyModelBase):
    """
    The Standardized Vocabularies contains records, or Concepts, that uniquely
    identify each fundamental unit of meaning used to express clinical information
    in all domain tables of the CDM. Concepts are derived from vocabularies, which
    represent clinical information across a domain (e.g. conditions, drugs, procedures)
    through the use of codes and associated descriptions. Some Concepts are designated
    Standard Concepts, meaning these Concepts can be used as normative expressions of a
    clinical entity within the OMOP Common Data Model and within standardized analytics.
    Each Standard Concept belongs to one domain, which defines the location where the
    Concept would be expected to occur within data tables of the CDM.

    Concepts can represent broad categories (like ‘Cardiovascular disease’),
    detailed clinical elements (‘Myocardial infarction of the anterolateral wall’)
    or modifying characteristics and attributes that define Concepts at various levels
    of detail (severity of a disease, associated morphology, etc.).

    Records in the Standardized Vocabularies tables are derived from national or
    international vocabularies such as SNOMED-CT, RxNorm, and LOINC, or custom
    Concepts defined to cover various aspects of observational data analysis.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT
    """

    __tablename__: Final[str] = "concept"

    # A unique identifier for each Concept across all domains.
    concept_id: Final[Column] = PKIntField(
        f"{VocabularyModelBase.metadata.schema}_{__tablename__}_id_seq"
    )

    # An unambiguous, meaningful and descriptive name for the Concept.
    concept_name: Final[Column] = CharField(255, nullable=False)

    # A foreign key to the DOMAIN table the Concept belongs to.
    # I am a Drug, Procedure, Condition, Anatomical site, etc...
    # NOTE: This seems odd, circular dependency with DOMAIN table
    domain_id: Final[Column] = CharField(
        20, FK("domain.domain_id"), nullable=False
    )

    # Vocab name used: NDC, ...
    # A foreign key to the VOCABULARY table indicating from which source the
    # Concept has been adapted.
    # NOTE: This seems odd, circular dependency with VOCABULARY table
    vocabulary_id: Final[Column] = CharField(
        20, FK("vocabulary.vocabulary_id"), nullable=False
    )

    # Class inside the vocab
    # The attribute or concept class of the Concept.
    # Examples are ‘Clinical Drug’, ‘Ingredient’, ‘Clinical Finding’ etc.
    # NOTE: This seems odd, circular dependency with VOCABULARY table
    concept_class_id: Final[Column] = CharField(
        20, FK("concept_class.concept_class_id"), nullable=False
    )

    # A flag to indicate if it is standard or not
    # This flag determines where a Concept is a Standard Concept, i.e. is used in the data,
    # a Classification Concept, or a non-standard Source Concept.
    # The allowable values are ‘S’ (Standard Concept) and ‘C’ (Classification Concept),
    # otherwise the content is NULL.
    standard_concept: Final[Column] = CharField(1)

    # Different from the ID: ID is unique and for OMOP, concept_code is from the original vocabulary
    # There can be many different concepts sharing the same concept_code.
    # The concept code represents the identifier of the Concept in the source vocabulary,
    # such as SNOMED-CT concept IDs, RxNorm RXCUIs etc. Note that concept codes are not unique across vocabularies.
    concept_code: Final[Column] = CharField(50, nullable=False)

    # Validity time - concepts are not eternal, they can come and go.
    # The date when the Concept was first recorded.
    # The default value is 1-Jan-1970, meaning, the Concept has no (known) date of inception.
    valid_start_date: Final[Column] = DateField(nullable=False)
    # The date when the Concept became invalid because it was deleted or superseded (updated)
    # by a new concept. The default value is 31-Dec-2099, meaning, the Concept is valid until it becomes deprecated.
    valid_end_date: Final[Column] = DateField(nullable=False)

    # If it is invalid because of the end date, this indicates the reason why
    # Reason the Concept was invalidated. Possible values are D (deleted),
    # U (replaced with an update) or NULL when valid_end_date has the default value.
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class Vocabulary(VocabularyModelBase):
    """
    The VOCABULARY table includes a list of the Vocabularies collected from
    various sources or created de novo by the OMOP community. This reference
    table is populated with a single record for each Vocabulary source and
    includes a descriptive name and other associated attributes for the Vocabulary.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#VOCABULARY
    """

    __tablename__: Final[str] = "vocabulary"

    # A unique identifier for each Vocabulary, such as ICD9CM, SNOMED, Visit.
    vocabulary_id: Final[Column] = PKCharField(20, "vocabulary_id_seq")

    # The name describing the vocabulary, for example International Classification
    # of Diseases, Ninth Revision, Clinical Modification, Volume 1 and 2 (NCHS) etc.
    vocabulary_name: Final[Column] = CharField(255, nullable=False)

    # External reference to documentation or available download of the about the vocabulary.
    vocabulary_reference: Final[Column] = CharField(255)

    # Version of the Vocabulary as indicated in the source.
    vocabulary_version: Final[Column] = CharField(255)

    # A Concept that represents the Vocabulary the VOCABULARY record belongs to.
    vocabulary_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model
@freeze_instance
class Domain(VocabularyModelBase):
    """
    The DOMAIN table includes a list of OMOP-defined Domains the
    Concepts of the Standardized Vocabularies can belong to.
    A Domain defines the set of allowable Concepts for the standardized fields in the CDM tables.
    For example, the “Condition” Domain contains Concepts that describe a condition of a patient,
    and these Concepts can only be stored in the condition_concept_id field of the CONDITION_OCCURRENCE
    and CONDITION_ERA tables. This reference table is populated with a single record for each Domain
    and includes a descriptive name for the Domain.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#DOMAIN
    """

    __tablename__: Final[str] = "domain"

    # A unique key for each domain.
    domain_id: Final[Column] = PKCharField(20, "domain_id_seq")

    # The name describing the Domain, e.g. Condition, Procedure, Measurement etc.
    domain_name: Final[Column] = CharField(255, nullable=False)

    # A Concept that represents the Concept Class.
    domain_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model
@freeze_instance
class ConceptClass(VocabularyModelBase):
    """
    The CONCEPT_CLASS table is a reference table, which includes a list of the
    classifications used to differentiate Concepts within a given Vocabulary.
    This reference table is populated with a single record for each Concept Class.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_CLASS
    """

    __tablename__: Final[str] = "concept_class"

    # A unique key for each class.
    concept_class_id: Final[Column] = PKCharField(20, "concept_class_id_seq")

    # The name describing the Concept Class, e.g. Clinical Finding, Ingredient, etc.
    concept_class_name: Final[Column] = CharField(255, nullable=False)

    # A Concept that represents the Concept Class.
    concept_class_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model
@freeze_instance
class ConceptRelationship(VocabularyModelBase, PKIdMixin):
    """
    The CONCEPT_RELATIONSHIP table contains records that define direct
    relationships between any two Concepts and the nature or type of the relationship.
    Each type of a relationship is defined in the RELATIONSHIP table.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_RELATIONSHIP
    """

    __tablename__: Final[str] = "concept_relationship"

    concept_id_1: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )
    concept_id_2: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # The relationship between CONCEPT_ID_1 and CONCEPT_ID_2.
    # Please see the Vocabulary Conventions. for more information.
    relationship_id: Final[Column] = CharField(
        20,
        FK("relationship.relationship_id"),
        nullable=False,
    )

    # The date when the relationship is first recorded.
    valid_start_date: Final[Column] = DateField(nullable=False)

    # The date when the relationship is invalidated
    valid_end_date: Final[Column] = DateField(nullable=False)

    # Reason the relationship was invalidated.
    # Possible values are ‘D’ (deleted), ‘U’ (updated) or NULL.
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class Relationship(VocabularyModelBase):
    """
    The RELATIONSHIP table provides a reference list of all types of
    relationships that can be used to associate any two concepts in
    the CONCEPT_RELATIONSHP table.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#RELATIONSHIP
    """

    __tablename__: Final[str] = "relationship"

    relationship_id: Final[Column] = PKCharField(20, "relationship_id_seq")

    relationship_name: Final[Column] = CharField(255, nullable=False)

    is_hierarchical: Final[Column] = CharField(1, nullable=False)

    defines_ancestry: Final[Column] = CharField(1, nullable=False)

    reverse_relationship_id: Final[Column] = CharField(20, nullable=False)

    relationship_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model
@freeze_instance
class ConceptSynonym(VocabularyModelBase, PKIdMixin):
    """
    The CONCEPT_SYNONYM table is used to store alternate names and descriptions for Concepts.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_SYNONYM
    """

    __tablename__: Final[str] = "concept_synonym"

    concept_id: Final[Column] = IntField(FK(Concept.concept_id), nullable=False)

    # The alias name
    concept_synonym_name: Final[Column] = CharField(1000, nullable=False)

    # The alias name
    language_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )


# do not register vocab models
# @register_analytical_model
@freeze_instance
class ConceptAncestor(VocabularyModelBase, PKIdMixin):
    """
    The CONCEPT_ANCESTOR table is designed to simplify observational analysis by
    providing the complete hierarchical relationships between Concepts.
    Only direct parent-child relationships between Concepts are stored
    in the CONCEPT_RELATIONSHIP table.
    To determine higher level ancestry connections, all individual direct
    relationships would have to be navigated at analysis time.
    The CONCEPT_ANCESTOR table includes records for all parent-child relationships,
    as well as grandparent-grandchild relationships and those of any other level of lineage.
    Using the CONCEPT_ANCESTOR table allows for querying for all descendants of a hierarchical concept.
    For example, drug ingredients and drug products are all descendants of a drug class ancestor.

    This table is entirely derived from the CONCEPT, CONCEPT_RELATIONSHIP and RELATIONSHIP tables.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#CONCEPT_ANCESTOR
    """

    __tablename__: Final[str] = "concept_ancestor"

    # The Concept Id for the higher-level concept that forms the ancestor in the relationship.
    ancestor_concept_id: Final[Column] = IntField(
        FK("concept.concept_id"), nullable=False
    )

    # The Concept Id for the lower-level concept that forms the descendant in the relationship.
    descendant_concept_id: Final[Column] = IntField(
        FK("concept.concept_id"), nullable=False
    )

    # The minimum separation in number of levels of hierarchy between ancestor and descendant concepts.
    # This is an attribute that is used to simplify hierarchic analysis.
    min_levels_of_separation: Final[Column] = IntField(nullable=False)

    # The maximum separation in number of levels of hierarchy between ancestor and descendant concepts.
    # This is an attribute that is used to simplify hierarchic analysis.
    max_levels_of_separation: Final[Column] = IntField(nullable=False)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class SourceToConceptMap(VocabularyModelBase, PKIdMixin):
    """
    The source to concept map table is a legacy data structure within the OMOP Common Data Model,
    recommended for use in ETL processes to maintain local source codes which are not available
    as Concepts in the Standardized Vocabularies, and to establish mappings for each source code
    into a Standard Concept as target_concept_ids that can be used to populate the Common Data
    Model tables. The SOURCE_TO_CONCEPT_MAP table is no longer populated with content within the
    Standardized Vocabularies published to the OMOP community.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#SOURCE_TO_CONCEPT_MAP
    """

    __tablename__: Final[str] = "source_to_concept_map"

    # The source code being translated into a Standard Concept.
    source_code: Final[Column] = CharField(50, nullable=False)

    # A foreign key to the Source Concept that is being translated into a Standard Concept.
    # NOTE: This is either 0 or should be a number above 2 billion, which are the
    # Concepts reserved for site-specific codes and mappings.
    source_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # A foreign key to the VOCABULARY table defining the vocabulary of the
    # source code that is being translated to a Standard Concept.
    # TO-DO: If the documentation mentions it is a FK why not provide this?
    source_vocabulary_id: Final[Column] = CharField(20, nullable=False)

    # An optional description for the source code.
    # This is included as a convenience to compare the description of the
    # source code to the name of the concept.
    source_code_description: Final[Column] = CharField(255)

    # The target Concept to which the source code is being mapped.
    target_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # The Vocabulary of the target Concept.
    target_vocabulary_id: Final[Column] = CharField(20, nullable=False)

    # The date when the mapping instance was first recorded.
    valid_start_date: Final[Column] = DateField(nullable=False)

    # The date when the mapping instance became invalid because it
    # was deleted or superseded (updated) by a new relationship.
    # Default value is 31-Dec-2099.
    valid_end_date: Final[Column] = DateField(nullable=False)

    # Reason the mapping instance was invalidated.
    # Possible values are D (deleted), U (replaced with an update)
    # or NULL when valid_end_date has the default value.
    invalid_reason: Final[Column] = CharField(1)


# do not register vocab models
# @register_analytical_model
@freeze_instance
class DrugStrength(VocabularyModelBase, PKIdMixin):
    """
    The DRUG_STRENGTH table contains structured content about the amount
    or concentration and associated units of a specific ingredient
    contained within a particular drug product. This table is supplemental
    information to support standardized analysis of drug utilization.

    https://ohdsi.github.io/CommonDataModel/cdm54.html#DRUG_STRENGTH
    """

    __tablename__: Final[str] = "drug_strength"

    # The Concept representing the Branded Drug or Clinical Drug Product.
    drug_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # The Concept representing the active ingredient contained within the drug product.
    # Combination Drugs will have more than one record in this table, one for
    # each active Ingredient.
    ingredient_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id), nullable=False
    )

    # The numeric value or the amount of active ingredient contained within the drug product.
    amount_value: Final[Column] = NumericField()

    # The Concept representing the Unit of measure for the amount of active ingredient
    # contained within the drug product.
    amount_unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))

    # The concentration of the active ingredient contained within the drug product.
    numerator_value: Final[Column] = NumericField()

    # The Concept representing the Unit of measure for the concentration of active ingredient.
    numerator_unit_concept_id: Final[Column] = IntField(FK(Concept.concept_id))

    # The amount of total liquid (or other divisible product, such as ointment, gel, spray, etc.).
    denominator_value: Final[Column] = NumericField()

    # The Concept representing the denominator unit for the concentration of active ingredient.
    denominator_unit_concept_id: Final[Column] = IntField(
        FK(Concept.concept_id)
    )

    # The number of units of Clinical Branded Drug or Quantified Clinical or
    # Branded Drug contained in a box as dispensed to the patient.
    box_size: Final[Column] = IntField()

    # The date when the Concept was first recorded. The default value is 1-Jan-1970.
    valid_start_date: Final[Column] = DateField(nullable=False)

    # The date when then Concept became invalid.
    valid_end_date: Final[Column] = DateField(nullable=False)

    # Reason the concept was invalidated. Possible values are D (deleted),
    # U (replaced with an update) or NULL when valid_end_date has the default value.
    invalid_reason: Final[Column] = CharField(1)


__all__ = [
    "Concept",
    "Vocabulary",
    "Domain",
    "ConceptClass",
    "ConceptRelationship",
    "Relationship",
    "ConceptSynonym",
    "ConceptAncestor",
    "SourceToConceptMap",
    "DrugStrength",
]
