import pickle

fields = """
CANCELLATION_DATE
DWELLING_DEDUCTIBLE_TYPE
LENDER_LISTED_AS_ADDITIONAL_NAMED_INSURED_DESCRIPTION
CHANGE_EFFECTIVE_DATE
COLLISION_COVERAGE
COMPREHENSIVE_COVERAGE
MOBILE_HOME_ADDRESS_CITY
MOBILE_HOME_ADDRESS_STATE
MOBILE_HOME_ADDRESS_STREET
MOBILE_HOME_ADDRESS_ZIP_CODE
NUMBER_OF_UNITS
CONDOMINIUM_ASSOCIATION_NAME
"""
fields = fields.split('\n')
fields.pop(0)
fields.pop(-1)

# with open('/home/users001/Desktop/data_set_RENEWAL.bin', 'rb') as fp:
#     data = pickle.load(fp)
#
#     new_data = []
#     new_data.extend(data)
#
#     for tup in data:
#         new_ents = []
#         entities = tup[1].get('entities')
#         new_ents.extend(entities)
#         if len(entities) > 1:
#             for ent in entities:
#                 if ent[2] in fields:
#                     new_ents.remove(ent)
#             for tup1 in new_data:
#                 if tup1 == tup:
#                     tup1[1]['entities'] = new_ents
#         else:
#             for ent in entities:
#                 if ent[2] in fields:
#                     new_data.remove(tup)
#
# with open('data_set_RENEWAL.bin', 'wb') as fw:
#     pickle.dump(new_data, fw)






with open('data_set_RENEWAL.bin', 'rb') as fp:
    data = pickle.load(fp)
    count = 0
    for tup in data:
        enities = tup[1].get('entities')
        for ent in enities:
            if ent[2] in fields:
                count += 1
                print(tup)

print(count)
