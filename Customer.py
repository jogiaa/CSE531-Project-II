import grpc
import bankingsystem_pb2
import bankingsystem_pb2_grpc
import json
from google.protobuf.json_format import MessageToDict
from BankingSystemUtility import log_msg, initializeLogging, parseInputFile, getProcessId, parseLocalArgs, \
    translateEntityToEnum, translateInterfaceToEnum


class Customer:
    def __init__(self, id, events):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = self.createStub()
        # customer clock
        self.clock = 0

    def createStub(self):
        processId = getProcessId(self.id)
        log_msg('Creating branch stub for customer {0} on {1}'.format(self.id, processId))
        return bankingsystem_pb2_grpc.TransactionStub(
            grpc.insecure_channel('[::]:{0}'.format(str(processId))))

    def executeEvents(self):
        log_msg('Sending out events for {0}'.format(self.id))
        resp = self.stub.MsgDelivery(
            bankingsystem_pb2.BankRequest(id=self.id, type=bankingsystem_pb2.customer, events=self.events,
                                          clock=self.clock))
        return resp

    def __str__(self):
        return 'CUSTOMER[id={0},events={1}, clock={2}'.format(self.id, str(self.events), str(self.clock))


def extractCustomerData(parsedData):
    customers = list()
    for data in parsedData:
        log_msg(data)
        if bankingsystem_pb2.customer == translateEntityToEnum(data['type']):
            log_msg('its a Customer with id : %s' % data['id'])
            events = data['events']
            for event in events:
                event['interface'] = translateInterfaceToEnum(event['interface'])
            cust = Customer(data['id'], events)
            customers.append(cust)

    return customers


def writeToOutputFile(allResponses, outputFileName):
    log_msg('Writing to output file')
    convertedResponses = list()
    for respo in allResponses:
        resToDict = MessageToDict(respo)
        for resp in resToDict['recv']:
            # Since proto3 doesn't write outfields with ZERO. This is to make sure that for 'money'
            # we get a value, even if it is ZERO.
            if resp['interface'] == 'query' and 'money' not in resp.keys():
                resp['money'] = 0
        convertedResponses.append(resToDict)

    with open(outputFileName, "a") as outFile:
        json.dump(convertedResponses, outFile)


if __name__ == '__main__':
    initializeLogging() # intializing the logs, so we can have detailed message along with the process IDs
    fileNames = parseLocalArgs() # parsing the arguments
    customers = extractCustomerData(parseInputFile(fileNames[0])) # extracting customers from the input file
    responses = list()
    for customer in customers:
        response = customer.executeEvents()
        # saving the timestamp from the server to update the local customer.
        customer.clock = response.clock
        responses.append(response) # collecting the responses so we can create a output json file
        log_msg(customer)
    # writing the responses in json file
    writeToOutputFile(responses, fileNames[1])
