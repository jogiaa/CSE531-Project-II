import json.decoder

import grpc
import bankingsystem_pb2
import bankingsystem_pb2_grpc
from concurrent import futures
import multiprocessing
from google.protobuf.json_format import MessageToDict
import json
from BankingSystemUtility import log_msg, initializeLogging, parseInputFile, getProcessId, parseLocalArgs, \
    translateEntityToEnum
from time import sleep


class Branch(bankingsystem_pb2_grpc.TransactionServicer):
    # constants for sub-events names
    WITHDRAW_REQUEST = 'withdraw_request'
    WITHDRAW_EXECUTE = 'withdraw_execute'
    WITHDRAW_PROPAGATE_REQUEST = 'withdraw_propagate_request'
    WITHDRAW_PROPAGATE_EXECUTE = 'withdraw_propagate_execute'
    WITHDRAW_PROPAGATE_RESPONSE = 'withdraw_propagate_response'
    WITHDRAW_RESPONSE = 'withdraw_response'

    DEPOSIT_REQUEST = 'deposit_request'
    DEPOSIT_EXECUTE = 'deposit_execute'
    DEPOSIT_PROPAGATE_REQUEST = 'deposit_propagate_request'
    DEPOSIT_PROPAGATE_EXECUTE = 'deposit_propagate_execute'
    DEPOSIT_PROPAGATE_RESPONSE = 'deposit_propagate_response'
    DEPOSIT_RESPONSE = 'deposit_response'

    def __init__(self, id, balance, branches):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = list()
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # This will keep track of the branch's internal logical clock as per Lamport algorithm.
        self.clock = 0
        # This dictionary will record all the sub-events happening on the incoming requests along with the timestamps.
        self.branchSubEventData = {'pid': self.id, 'data': list()}

    def MsgDelivery(self, request, context):
        """
        The RPC call to handle the incoming requests.
        :param request: un-marsheled BankRequest received by the RPC call.
        :param context: Server context
        :return: BankResponse
        """
        log_msg('request::' + json.dumps(MessageToDict(request)))
        responseList = list()
        # protobuf does not translate ZERO values. This is to make sure if that happens we have a default request
        # clock value so we can increment it.
        if not request.clock:
            request.clock = 0
        eventId = 0
        eventInterface = 0
        for event in request.events:
            # we are only interested in deposit and withdraw interface for propagation. This will keep track of that
            if bankingsystem_pb2.deposit == event.interface or bankingsystem_pb2.withdraw == event.interface:
                eventInterface = event.interface
                eventId = event.id
            # Query interface
            if bankingsystem_pb2.query == event.interface:
                # Mandatory sleep after query interface so the results can be propagated.
                sleep(3)
                responseList.append(self.query())
            # deposit interface
            elif bankingsystem_pb2.deposit == event.interface:
                # syncing clock on the eventReceive sub-event
                self.clockEventRequest(request.clock)
                # logging the sub-event
                self.logBranchSubEventData(eventId=event.id, subEventName=self.DEPOSIT_REQUEST)
                # executing the deposit interface
                depositResponse = self.deposit(event)
                # adding the response of deposit to list to construct the final response for this request
                responseList.append(depositResponse)
                # calling the function to propagate if the response of the deposit was successful.
                self.propagateIfOperationSuccessful(depositResponse, event, bankingsystem_pb2.propagate_deposit)
            # withdraw interface
            elif bankingsystem_pb2.withdraw == event.interface:
                # syncing clock on the eventReceive sub-event
                self.clockEventRequest(request.clock)
                # logging the sub-event
                self.logBranchSubEventData(eventId=event.id, subEventName=self.WITHDRAW_REQUEST)
                # executing the withdraw interface
                withdrawRes = self.withdraw(event)
                # adding the response of withdraw to list to construct the final response for this request
                responseList.append(withdrawRes)
                # calling the function to propagate if the response of the deposit was successful.
                self.propagateIfOperationSuccessful(withdrawRes, event, bankingsystem_pb2.propagate_withdraw)
            # propagate deposit interface
            elif bankingsystem_pb2.propagate_deposit == event.interface:
                # syncing clock on the Propagate_Request sub-event
                self.clockPropagateRequest(request.clock)
                # logging the sub-event
                self.logBranchSubEventData(eventId=event.id, subEventName=self.DEPOSIT_PROPAGATE_REQUEST)
                # adding the response to list to construct the final response for this request
                responseList.append(self.handlePropagateDeposit(event))
            # propagate withdraw interface
            elif bankingsystem_pb2.propagate_withdraw == event.interface:
                # syncing clock on the Propagate_Request sub-event
                self.clockPropagateRequest(request.clock)
                # logging the sub-event
                self.logBranchSubEventData(eventId=event.id, subEventName=self.WITHDRAW_PROPAGATE_REQUEST)
                # adding the response to list to construct the final response for this request
                responseList.append(self.handlePropagateWithdraw(event))

        log_msg('going to log {0} for event id {1}'.format(str(eventInterface), str(eventId)))
        # if the interface is withdraw or deposit then we sync the clock for eventResponse and log the sub-events
        if bankingsystem_pb2.withdraw == eventInterface:
            log_msg('Logging withdraw')
            self.clockEventResponse()
            self.logBranchSubEventData(eventId=eventId, subEventName=self.WITHDRAW_RESPONSE)
        elif bankingsystem_pb2.deposit == eventInterface:
            log_msg('Logging deposit')
            self.clockEventResponse()
            self.logBranchSubEventData(eventId=eventId, subEventName=self.DEPOSIT_RESPONSE)

        # writing the branch output to the file as json
        self.writeServerOutPut()
        # constructing the final response
        bankResponse = bankingsystem_pb2.BankResponse(id=request.id, recv=responseList, clock=self.clock)

        log_msg('Response from Branch#{0}:{1}'.format(self.id, json.dumps(MessageToDict(bankResponse))))
        log_msg('*' * 20)
        log_msg(json.dumps(self.branchSubEventData))
        log_msg('*' * 20)
        return bankResponse

    def writeServerOutPut(self):
        """
        Writes the subevent data is desired format to the output json file.
        :return: None
        """
        log_msg('^' * 20)
        dataToWrite = list()
        outFileName = "serverOut.json"
        branchFound = False
        # this list will contain all the subevents fro the branches, we can sort it later on based on clock values
        events = list()
        try:
            with open(outFileName, "r") as file:
                existingData = json.load(file)
                log_msg('Existing data in {0} file ->{1}'.format(outFileName, existingData))
                for d in existingData:
                    # the output json file can contain two type of objects, one for branches and other for events
                    # at this point we are trying to gather the branch data only. We can construct
                    # the event data based on the branch data later on
                    if 'pid' in d:
                        # update the 'data' field only if the branch id is same as pid
                        if d['pid'] == self.id:
                            d['data'] = self.branchSubEventData['data']
                            branchFound = True
                        dataToWrite.append(d)
                        # add to list so we can construct event data later on
                        events.extend(d['data'])
                    else:
                        log_msg('Not branch data{0}'.format(d))
                # it means the existing data in json file is not for the branch currently running. We need to
                # add it to final data
                if not branchFound:
                    dataToWrite.append(self.branchSubEventData)
                    events.extend(self.branchSubEventData['data'])
        except Exception as err:
            # First time around we might not have the file, so will get file not found exception here
            log_msg('------>{0}'.format(err))
            # but we need to add data so we can write it for the first time
            dataToWrite.append(self.branchSubEventData)
            events.extend(self.branchSubEventData['data'])
            pass
        # adding the subevent data sorted by clock to the final dict to be written to the file
        dataToWrite.extend(self.makeSortedEventData(events))
        # writing to file
        outfile = open(outFileName, 'w')
        json.dump(dataToWrite, outfile)
        outfile.close()
        log_msg('^' * 20)
        # cautionary sleep after writing to the file
        sleep(1)

    def makeSortedEventData(self, events):
        """
        This function will receive the subevents data from the all the branches happened so far read from the output
        json file. We will sort the available data as per the clock values and group them by the event IDs
        :param events: List of all the subevents
        :return: list of dictionaries grouped by events and sorted by clock values
        """
        finalResult = {}
        # sorting the subevents by the clock values regardless to the eventIds
        events = sorted(events, key=lambda i: i['clock'])
        for eventData in events:
            # making the event Id as key in order to create the unique entries
            mainKey = str(eventData['id'])
            # if a event was not already in the list we will need to create it
            if mainKey not in finalResult:
                finalResult[mainKey] = {'eventid': eventData['id'], 'data': list()}

            # adding the subevent data
            finalResult[mainKey]['data'].append({'clock': eventData['clock'], 'name': eventData['name']})

        return list(finalResult.values())

    def logBranchSubEventData(self, eventId, subEventName):
        """
        This will populate the self.branchSubEventData for all the subevents along with the eventId and clock details
        :param eventId: Unique ID of the event
        :param subEventName: Name of sub event
        :return: None
        """
        self.branchSubEventData['data'].append({'id': eventId, 'name': subEventName, 'clock': self.clock})
        self.branchSubEventData['data'] = sorted(self.branchSubEventData['data'], key=lambda i: i['clock'])

    def query(self):
        """

        :return:
        """
        log_msg('-----QUERY-------')
        res = {'interface': bankingsystem_pb2.query, 'result': bankingsystem_pb2.success, 'money': self.balance}
        return res

    def deposit(self, event):
        """
        Depositing the money to the branch. And calling the eventExecute sub-interface. This function will also log the
        subevent.
        :param event: The incoming event which contains the money and event IDs
        :return: outputEvent in dictionary form
        """
        log_msg('-----DEPOSIT-------')
        if event.money > 0:
            self.balance += event.money
            opResult = bankingsystem_pb2.success
        else:
            log_msg('Deposit amount negative.Operation failed.')
            opResult = bankingsystem_pb2.failure

        # logging and calling the eventExecute interface
        self.clockEventExecute()
        self.logBranchSubEventData(eventId=event.id, subEventName=self.DEPOSIT_EXECUTE)

        return {'interface': bankingsystem_pb2.deposit, 'result': opResult}

    def withdraw(self, event):
        """
        Withdrawing money from the branch. And calling the eventExecute sub-interface. This function will also log the
        subevent.
        :param event: The incoming event which contains the money and event IDs
        :return: outputEvent in dictionary form
        """
        log_msg('-----WITHDRAW-------')
        if event.money < 0 or self.balance - event.money < 0:
            log_msg('Insufficient funds for Withdraw.Operation failed.')
            opResult = bankingsystem_pb2.failure
        else:
            self.balance -= event.money
            opResult = bankingsystem_pb2.success

        # logging and calling the eventExecute interface
        self.clockEventExecute()
        self.logBranchSubEventData(eventId=event.id, subEventName=self.WITHDRAW_EXECUTE)

        return {'interface': bankingsystem_pb2.withdraw, 'result': opResult}

    def propagateIfOperationSuccessful(self, operationResponse, event, propagateOperation):
        """
        Propage the interface if operation was successful. And advances the clock
        :param operationResponse:
        :param event:
        :param propagateOperation:
        :return:
        """
        if operationResponse['result'] == bankingsystem_pb2.success:
            self.propagateEvent(propagateOperation, event.money, event.id)
        else:
            log_msg('{0} operation failed. No propagation required.'.format(event.interface))

    def handlePropagateDeposit(self, event):
        """
        Handle the deposit propagation request and call the subevent Propagate_Execute.It will also log
            deposit_propagate_execute
        :param event:
        :return:
        """
        log_msg('-----Handling Propagated Deposit-------')
        self.balance += event.money
        # calling and logging Propagate_Execute
        self.clockPropagateExecute()
        self.logBranchSubEventData(eventId=event.id, subEventName=self.DEPOSIT_PROPAGATE_EXECUTE)
        return {'interface': bankingsystem_pb2.propagate_deposit, 'result': bankingsystem_pb2.success}

    def handlePropagateWithdraw(self, event):
        """
        Handle the withdraw propagation request and call the subevent Propagate_Execute.It will also log
        withdraw_propagate_execute
        :param event:
        :return:
        """
        log_msg('-----Handling Propagated Withdraw-------')
        self.balance -= event.money
        # calling and logging Propagate_Execute
        self.clockPropagateExecute()
        self.logBranchSubEventData(eventId=event.id, subEventName=self.WITHDRAW_PROPAGATE_EXECUTE)
        return {'interface': bankingsystem_pb2.propagate_withdraw, 'result': bankingsystem_pb2.success}

    def propagateEvent(self, eventType, amount, eventId):
        """
        This function is called when withdraw or deposit operation is successful. This will propagate that operation
        to other branches. This function will also call and log the subinterface PROPAGATE_RESPONSE when response is
        received from propagation.
        :param eventType:
        :param amount:
        :return:
        """
        log_msg(f'-----Propagating Event {eventType} for amount {amount}-------')
        for bid in self.branches:
            portId = getProcessId(bid)

            log_msg('Propagating to branch {0} on port {1}'.format(bid, portId))
            with grpc.insecure_channel(f'localhost:{str(portId)}') as channel:
                stub = bankingsystem_pb2_grpc.TransactionStub(channel)
                req = [{"id": eventId, "interface": eventType, "money": amount}]
                response = stub.MsgDelivery(
                    bankingsystem_pb2.BankRequest(id=bid, type=bankingsystem_pb2.branch, events=req,
                                                  clock=self.clock))
                log_msg('Response on propagation {0} from branch {1}'.format(json.dumps(MessageToDict(response)), bid))

                # calling the PROPAGATE_RESPONSE sub event and loggin it
                self.clockPropagateResponse(response.clock)
                if eventType == bankingsystem_pb2.propagate_withdraw:
                    self.logBranchSubEventData(eventId=eventId, subEventName=self.WITHDRAW_PROPAGATE_RESPONSE)
                elif eventType == bankingsystem_pb2.propagate_deposit:
                    self.logBranchSubEventData(eventId=eventId, subEventName=self.DEPOSIT_PROPAGATE_RESPONSE)

    def __str__(self):
        return "BRANCH[id = {0}, balance = {1} , branches = {2} , tick={3}".format(self.id,
                                                                                   self.balance,
                                                                                   str(self.branches), str(self.clock))

    def clockEventRequest(self, remoteClock):
        """
        This is implementation of first subinterface Event_Request and propagationRecieved. This subevent happens
        when the Branch process receives a request from the Customer process.
        The Branch process selects the larger value between the local clock
        and the remote clock from the message, and increments one from the selected value.
        :param remoteClock: Remote clock
        :return: None
        """
        self.selectMaxClockAndAdvance(remoteClock=remoteClock, subEventName="Event_Request")

    def clockEventExecute(self):
        """
        This is implementation of second subinterface Event_Execute.This subevent happens when the Branch process
        executes the event after the subevent  “Event Request”. The Branch process increments one from its local clock.
        :return: None
        """
        self.advanceClock("Event_Execute")

    def clockPropagateRequest(self, remoteClock):
        """
        Implantation of third subinterface Propagate_Request.This subevent happens when the
        Branch process recieves the propagation request from its fellow branch processes.
        The Branch process selects the larger value between the local clock
        and the remote clock from the message, and increments one from the selected value.
        :param remoteClock: Remote clock
        :return: None
        """
        self.selectMaxClockAndAdvance(remoteClock=remoteClock, subEventName="Propagate_Request")

    def clockPropagateResponse(self, remoteClock):
        """
        Implantation of fourth subinterface Propagate_Response.
        This subevent happens when the Branch receives the result of the subevent
        “Propogate_Execute” from its fellow branches.
        The Branch process selects the biggest value between the
        local clock and the remote clock from the message, and increments one from the selected value.
        :param remoteClock: Remote clock
        :return: None
        """
        self.selectMaxClockAndAdvance(remoteClock=remoteClock, subEventName="Propagate_Response")

    def clockPropagateExecute(self):
        """
        Implantation of fifth subinterface Propogate_Execute.
        This subevent happens when the Branch process executes the event after the subevent “Propogate_Request”.
        The Branch process increments one from its local clock.
        :return: None
        """
        self.advanceClock("Propogate_Execute")

    def clockEventResponse(self):
        """
        Implantation of sixth subinterface Event_Response.
        This subevent happens after all the propagation responses are returned from the branches.
        The branch returns success / fail back to the Customer process.
        The Branch process increments one from its local clock.
        :return: None
        """
        self.advanceClock("Event_Response")

    def advanceClock(self, subEventName):
        """
        Advance the clock by 1 tick
        :param subEventName: sub-event name for logging purposes
        :return: None
        """
        self.clock += 1
        log_msg("CLOCK:{0}:{1}".format(subEventName, str(self.clock)))

    def selectMaxClockAndAdvance(self, remoteClock, subEventName):
        """
        Take the maximum of the remote clock and local clock and advance by 1 tick.
        :param remoteClock: Remote clock value
        :param subEventName: sub-event name for logging purposes
        :return: None
        """
        self.clock = max(self.clock, remoteClock)
        self.advanceClock(subEventName)


def extractBranchData(parsedData):
    branches = list()
    branchIds = list()
    for data in parsedData:
        log_msg(data)
        if bankingsystem_pb2.branch == translateEntityToEnum(data['type']):
            log_msg('its a branch with id : %s' % data['id'])
            branches.append(Branch(id=data['id'], balance=data['balance'], branches=None))
            branchIds.append(data['id'])

    for branch in branches:
        branch.branches = [id for id in branchIds if branch.id != id]
        log_msg(branch)

    return branches


def startBranch(branch):
    portNumber = getProcessId(branch.id)
    log_msg(
        'Starting Branch server for {0} with balance {1} on port {2}'.format(branch.id, branch.balance, portNumber))
    options = (('grpc.so_reuseport', 1),)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=options)
    bankingsystem_pb2_grpc.add_TransactionServicer_to_server(branch, server)
    server.add_insecure_port('[::]:{0}'.format(portNumber))
    server.start()
    server.wait_for_termination()
    log_msg("--------" * 40)
    log_msg(branch.branchEventData)
    log_msg("--------" * 40)


if __name__ == '__main__':
    initializeLogging()
    inputFile = parseLocalArgs()
    branches = extractBranchData(parseInputFile(inputFile[0]))

    workers = []
    for branch in branches:
        # NOTE: It is imperative that the worker subprocesses be forked before
        # any gRPC servers start up. See
        # https://github.com/grpc/grpc/issues/16001 for more details.
        worker = multiprocessing.Process(target=startBranch,
                                         args=(branch,))
        worker.start()
        workers.append(worker)
    for worker in workers:
        worker.join()
