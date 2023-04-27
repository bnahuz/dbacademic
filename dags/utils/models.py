

from rdflib import Namespace, Literal, URIRef
from simpot import RdfsClass, BNamespace, serialize_to_rdf, graph
from rdflib.namespace import DC, FOAF

from hashlib import md5

def hashcode(institute, collection,  code):
    return md5(f"{institute}{collection}{code}".encode()).hexdigest()

#constants
UNIDADES = "UNIDADES"
DOCENTES = "DOCENTES"
CURSOS = "CURSOS"
DISCENTES = "DISCENTES"


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
    codigo_lotacao = CCSO.memberOf
    instituicao = CCSO.worksFor
    email = FOAF.mbox
    

    @RdfsClass(CCSO.Professor, "https://purl.org/dbacademic/resource#")
    @BNamespace('ccso', CCSO)
    @BNamespace('foaf', FOAF)
    def __init__ (self, dict):
        self.id = hashcode ( dict["instituicao"], DOCENTES, dict["id"])
        self.nome = Literal (dict["nome"])
        self.matricula = Literal(dict["matricula"])
        if "sexo" in dict:
            self.sexo = Literal(dict["sexo"])
        if "formacao" in dict and dict["formacao"] != None and dict["formacao"].upper() in formacao_dic:
            self.formacao = URIRef(formacao_dic[dict["formacao"].upper()])
        if "codigo_lotacao" in dict and dict["codigo_lotacao"] != None:
            hash_code = hashcode ( dict["instituicao"], UNIDADES , dict["codigo_lotacao"])
            self.codigo_lotacao = URIRef( f"https://purl.org/dbacademic/resource#{hash_code}" )
        if "instituicao" in dict and dict["instituicao"] != None:
            self.instituicao = URIRef(dict["instituicao"])
        
        if "email" in dict and dict["email"] != None:
            self.email = URIRef(dict["email"])


    def __repr__(self) -> str:
        return str ((self.nome, self.matricula))

class Discentes:

    nome = FOAF.name
    data_ingresso = CCSO.enrollmentDate
    matricula  = CCSO.personID
    codigo_curso = CCSO.enrolledIn
    sexo = FOAF.gender
 

    @RdfsClass(CCSO.Student, "https://purl.org/dbacademic/resource#")
    @BNamespace('ccso', CCSO)
    @BNamespace('foaf', FOAF)
    def __init__ (self, dict):
        self.id = hashcode ( dict["instituicao"], DISCENTES, dict["id"])
        self.nome = Literal (dict["nome"])
        self.matricula = Literal(dict["matricula"])
        if "sexo" in dict:
            self.sexo = Literal(dict["sexo"])
        if "codigo_curso" in dict and dict["codigo_curso"] != None:
            hash_code = hashcode ( dict["instituicao"], CURSOS , dict["codigo_curso"])
            self.codigo_curso = URIRef( f"https://purl.org/dbacademic/resource#{hash_code}" )
        if "data_ingresso" in dict:
            self.data_ingresso = Literal (dict["data_ingresso"])


class Cursos:

    nome = CCSO.psName
    codigo  = CCSO.code
    codigo_unidade = CCSO.offeredBy
    sameas = OWL.sameas
    codigo_coordenador = AIISO.responsibilityOf

    @RdfsClass(CCSO.ProgramofStudy, "https://purl.org/dbacademic/resource#")
    @BNamespace('ccso', CCSO)
    @BNamespace('foaf', FOAF)
    @BNamespace('owl', OWL)
    def __init__ (self, dict):
        self.id =  hashcode ( dict["instituicao"], CURSOS, dict["id"])
        self.codigo = Literal (str(dict["codigo"]))
        self.nome = Literal (dict["nome"])
        if "codigo_unidade" in dict and dict["codigo_unidade"] != None:
            hash_code = hashcode ( dict["instituicao"], UNIDADES , dict["codigo_unidade"])
            self.codigo_unidade = URIRef( f"https://purl.org/dbacademic/resource#{hash_code}" )
        
        if "sameas" in dict:
            self.sameas = URIRef(dict["sameas"])

        #if "coordenador" in dict:
        #    self.codigo_coordenador = URIRef(dict["coordenador"])

    

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
