from lxml import etree
from processa_dados_excel import DataProcessor
from collections import defaultdict

def indent(elem, level=0):
    """Adiciona indentação e quebras de linha para pretty print manual."""
    i = "\n" + level * "    "
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = i + "    "
        for e in elem:
            indent(e, level + 1)
        if not elem.tail or not elem.tail.strip():
            elem.tail = i
    else:
        if not elem.tail or not elem.tail.strip():
            elem.tail = i

def typeCust_mapping(DataType):
    if DataType == "NUMBER":
        return "INTEGER"
    elif DataType.startswith("VARCHAR"):
        return "STRING"
    elif DataType.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    else:
        return "CAIU NO CASO QUE TEM QUE VER/ typeCust_mapping"
    
def gambiarra_tv(DataType):
    if DataType == "NUMBER":
        return "ACC"
    elif DataType.startswith("VARCHAR"):
        return "STRING"
    elif DataType.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    else:
        return "CAIU NO CASO QUE TEM QUE VER/ gambiarra_tv"  
    
def gambiarra_uv(DataType):
    if DataType == "NUMBER":
        return "PDF[256]"
    elif DataType.startswith("VARCHAR"):
        return "STRING"
    elif DataType.startswith("TIMESTAMP"):
        return "TIMESTAMP"
    else:
        return "CAIU NO CASO QUE TEM QUE VER/ gambiarra_uv"

lista_inv_name = [
    "NR_NRCellDU_EBS",
    "NR_NRCellDU_EBS_PLMN",
    "NR_NRCellCU_EBS",
    "NR_NRCellCU_EBS_PLMN"
]

pack = 'VIVO_Altaia_PackR5GEricsson_NR24Q2_v1.29.xlsx'
# cria o processador
processor = DataProcessor(pack, lista_inv_name)

# roda o processamento e captura o resultado
data_sources_list, data_sources_attr_list = processor.process_data()

# caminho do arquivo existente
input_file = "ERICSSON_OSS_RAN_EBS_5G_oss.xml"
output_file = "ERICSSON_OSS_RAN_EBS_5G_oss_saida.xml"  # pode sobrescrever o mesmo ou salvar com outro nome

# carrega o XML existente
tree = etree.parse(input_file)
root = tree.getroot()


attr_dict = defaultdict(list)
for dsal in data_sources_attr_list:
    attr_dict[dsal[0]].append(dsal)

# Filtra colunas com dbn0type != "-" e ordena PK primeiro
for key, attr_list in attr_dict.items():
    # remove elementos com dbn0type "-"
    attr_list[:] = [col for col in attr_list if col[4] != "-"]
    # ordena PK primeiro
    attr_list.sort(key=lambda x: x[4] != "PK")

# percorre data_sources_list
for dsl in data_sources_list:
    key = dsl[0]

    inventoryName = dsl[0] 
    tableName = dsl[1]

    ossid = dsl[2]
    
    schema = dsl[3]
    description = dsl[4]
    period = dsl[5]
    delay = dsl[6]
    vendor = dsl[7]
    tecnologiaGrupoDeContadores = dsl[8]
    tableGroup = dsl[9]
        
    # cria a tabela
    unit = etree.Element(
        "unit",
        desc= description,
        id= ossid,
        measuredobjects="Vazio",
        name= ossid.capitalize(),
        ossId= ossid,
        tech= "EBS_5G"
    )

    # pega os atributos conectados (N)
    for dsal in attr_dict.get(key, []):
        SourceName = dsal[0]
        AttributeCounterName = dsal[1]
        AttributeCounterPhysicalName = dsal[2]
        DataType = dsal[3]
        MediationType = dsal[4]
        MetricsAttributeType = dsal[5]
        AltaiaAttributeType = dsal[6]
        Description = dsal[7]
        Example = dsal[8]
        
        etree.SubElement(
            unit, "item",
            desc= Description,
            id= AttributeCounterPhysicalName,
            name= AttributeCounterName,
            seqlength="Single",
            typeCust= typeCust_mapping(DataType),
            typeVendor= gambiarra_tv(DataType),
            unitVendor= gambiarra_uv(DataType),
            v= "EBS23Q3/EBS23Q3"
        )

    # adiciona a nova tabela no XML já existente
    root.append(unit)

# aplica indentação manual para cada elemento
indent(root)

# salva o XML atualizado
tree = etree.ElementTree(root)
tree.write(
    output_file,
    pretty_print=False,  # desliga o pretty_print do lxml
    xml_declaration=True,
    encoding="utf-8"
)

print(f"Tabela adicionada e salva em {output_file}")
