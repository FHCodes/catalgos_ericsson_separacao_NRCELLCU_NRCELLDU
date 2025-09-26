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

def gambiarra_measuredobjects(tb: str) -> str:
    if "NRCELLDU" in tb.upper():
        return "ManagedElement-GNBCUCPFunction-NRCellDU"
    elif "NRCELLCU" in tb.upper():
        return "ManagedElement-GNBCUCPFunction-NRCellCU"
    else:
        return "N/A"
    
def gambiarra_descricao(tb,descricao: str) -> str:
    if descricao is None or descricao.strip() == "":
        if "NRCELLDU" in tb.upper():
            return "ManagedElement-GNBCUCPFunction-NRCellDU"
        elif "NRCELLCU" in tb.upper():
            return "ManagedElement-GNBCUCPFunction-NRCellCU"
        else:
            return "N/A"
    return descricao

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
"NR_NRCellCU_EBS_PLMN",
"NR_NRCellDU_EBS_pmEbsnRlcDelayTimeDlQos_PLMN",
"NR_NRCellDU_EBS_pmEbsnRlcDelayTimeDlDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnRlcDelayPktTransmitDlQos_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacVolDlDrbQos_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacTimeDlDrbQos_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacLatTimeDlDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerUlQpskDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerUl64QamDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerUl256QamDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerUl16QamDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerDlQpskDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerDl64QamDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerDl256QamDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsnMacBlerDl16QamDistr_PLMN",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilUlResidualPartitionDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilUlPriorityPartitionDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilUlPartitionDistr_PLMNSlice",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilUlDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilUlCaSCellPartitionDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilDlResidualPartitionDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilDlPriorityPartitionDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilDlPartitionDistr_PLMNSlice",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilDlDistr",
"NR_NRCellDU_EBS_pmEbsMacRBSymUtilDlCaSCellPartitionDistr",
"NR_NRCellCU_EBS_pmEbsSessionTimeDrb5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPktLossUlXnUDistr_PLMN",
"NR_NRCellCU_EBS_pmEbsnPktLossUlX2UDistr_PLMN",
"NR_NRCellCU_EBS_pmEbsnPktLossDlXnUDistr_PLMN",
"NR_NRCellCU_EBS_pmEbsnPktLossDlX2UDistr_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolTransDlXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolTransDlX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolTransDlRetransXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolTransDlRetransX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolTransDlAggrXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolTransDlAggrX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolRecUlXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpVolRecUlX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlRetransXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlRetransX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlDiscXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlDiscX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlAggrXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlAggrX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlAckXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktTransDlAckX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktRecUlXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktRecUlX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktLossUlXnU5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsnPdcpPktLossUlX2UQci_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbRelNormal5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbRelAbnormalGnbAct5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbRelAbnormalGnb5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbRelAbnormalAmfAct5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbRelAbnormalAmf5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbEstabSuccInit5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbEstabSucc5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbEstabAttInit5qi_PLMN",
"NR_NRCellCU_EBS_pmEbsDrbEstabAtt5qi_PLMN"
]

pack = 'VIVO_Altaia_PackR5GEricsson_NR24Q2_v1.29.xlsx'
# cria o processador
processor = DataProcessor(pack, lista_inv_name)

# roda o processamento e captura o resultado
data_sources_list, data_sources_attr_list = processor.process_data()

# caminho do arquivo existente
input_file = "catalogos_base/ERICSSON_OSS_RAN_EBS_5G_oss.xml"
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
        desc= gambiarra_descricao(tableName,description),
        id= ossid,
        measuredobjects=gambiarra_measuredobjects(tableName),
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
