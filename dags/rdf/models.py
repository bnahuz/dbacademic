

from rdflib import Namespace, Literal, URIRef
from simpot import RdfsClass, BNamespace, serialize_to_rdf, graph
from rdflib.namespace import DC, FOAF

#import config

CCSO = Namespace ("https://w3id.org/ccso/ccso#")
SCHEMA = Namespace ("http://schema.org/legalName")
OWL = Namespace("http://www.w3.org/TR/owl-ref/")
AIISO = Namespace("http://purl.org/vocab/aiiso/schema#")


formacao_dic = {
    "MESTRADO": "https://w3id.org/ccso/ccso#Master",
    "DOUTORADO":"https://w3id.org/ccso/ccso#Doctorate",
    "ESPECIALIZAÇÃO": "https://w3id.org/ccso/ccso#Certificate",
    "GRADUAÇÃO":"https://w3id.org/ccso/ccso#Bachelor",
    "DESCONHECIDA": None,
}


class Docentes:
    nome = FOAF.name
    matricula  = CCSO.personID
    sexo = FOAF.gender
    formacao = CCSO.hasDegree
    lotacao = CCSO.memberOf
    instituicao = CCSO.worksFor
    

    @RdfsClass(CCSO.Professor, "https://purl.org/dbacademic/resource#")
    @BNamespace('ccso', CCSO)
    @BNamespace('foaf', FOAF)
    def __init__ (self, dict):
        self.id = dict["id"]
        self.nome = Literal (dict["nome"])
        self.matricula = Literal(dict["matricula"])
        if "sexo" in dict:
            self.sexo = Literal(dict["sexo"])
        if "formacao" in dict and dict["formacao"] != None:
            self.formacao = URIRef(formacao_dic[dict["formacao"]])
        if "lotacao" in dict and dict["lotacao"] != None:
            self.lotacao = URIRef(dict["lotacao"])
        if "instituicao" in dict and dict["instituicao"] != None:
            self.instituicao = URIRef(config.instituicoes_urls[dict["instituicao"].upper()])


    def __repr__(self) -> str:
        return str ((self.nome, self.matricula))

class Discentes:

    nome = FOAF.name
    data_ingresso = CCSO.enrollmentDate
    matricula  = CCSO.personID
    curso = CCSO.enrolledIn
    sexo = FOAF.gender
 

    @RdfsClass(CCSO.Student, "https://purl.org/dbacademic/resource#")
    @BNamespace('ccso', CCSO)
    @BNamespace('foaf', FOAF)
    def __init__ (self, dict):
        self.id = dict["id"]
        self.nome = Literal (dict["nome"])
        self.matricula = Literal(dict["matricula"])
        if "sexo" in dict:
            self.sexo = Literal(dict["sexo"])
        if "curso" in dict and dict["curso"] != None:
            self.curso = URIRef(dict["curso"])
        if "data_ingresso" in dict:
            self.data_ingresso = Literal (dict["data_ingresso"])


class Cursos:

    nome = CCSO.psName
    codigo  = CCSO.code
    unidade = CCSO.offeredBy
    sameas = OWL.sameas
    coordenador = AIISO.responsibilityOf

    @RdfsClass(CCSO.ProgramofStudy, "https://purl.org/dbacademic/resource#")
    @BNamespace('ccso', CCSO)
    @BNamespace('foaf', FOAF)
    @BNamespace('owl', OWL)
    def __init__ (self, dict):
        self.id = dict["id"]
        self.codigo = Literal (str(dict["codigo"]))
        self.nome = Literal (dict["nome"])
        if "unidade" in dict and dict["unidade"] != None:
            self.unidade = URIRef(dict["unidade"])
        
        if "sameas" in dict:
            self.sameas = URIRef(dict["sameas"])

        if "coordenador" in dict:
            self.sameas = URIRef(dict["coordenador"])

    

# englobar unidade, subunidade ...
#EducationalOrganization
class Unidades:
    nome = SCHEMA.legalName
    codigo = SCHEMA.identifier
    instituicao = CCSO.belongsTo

    @RdfsClass(CCSO.EducationalOrganization, "https://purl.org/dbacademic/resource#")
    @BNamespace('schema', SCHEMA)
    @BNamespace('ccso', CCSO)
    def __init__ (self, dict):
        self.id = dict["id"]
        self.codigo = Literal (str(dict["codigo"]))
        self.nome = Literal (dict["nome"])
        self.instituicao = URIRef(dict["instituicao"])
