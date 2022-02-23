package by.gsyrov.bamboolead.application.api.click_transaction.service;

import static by.gsyrov.bamboolead.application.api.click_transaction.utils.ClickTransactionUtils.convertToAllClickTransactionFilter;
import static java.util.stream.Collectors.toList;
import static org.springframework.data.domain.PageRequest.of;

import by.gsyrov.bamboolead.application.api.click_transaction.payload.request.AllClickTransactionFilterRequest;
import by.gsyrov.bamboolead.application.api.click_transaction.payload.response.ClickTransactionAdminResponse;
import by.gsyrov.bamboolead.application.api.click_transaction.payload.response.ClickTransactionPublisherResponse;
import by.gsyrov.bamboolead.application.api.offer.service.OfferService;
import by.gsyrov.bamboolead.application.api.user.service.SystemUserService;
import by.gsyrov.bamboolead.click.ClickServiceGrpc.ClickServiceBlockingStub;
import by.gsyrov.bamboolead.click.ClickTransactionContainer;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ClickTransactionService {

  private final ModelMapper mapper;
  private final OfferService offerService;
  private final SystemUserService systemUserService;

  @GrpcClient("ClickService")
  private ClickServiceBlockingStub stub;

  public Page<?> getAllClickTransaction(final AllClickTransactionFilterRequest filterRequest) {
    final Boolean isAdmin = systemUserService.isCurrentUserAdmin();
    final ClickTransactionContainer res = stub
        .getAllClickTransactions(convertToAllClickTransactionFilter(filterRequest,
            systemUserService.getPublisherNameMap(),
            offerService.getOfferNameMap(null)));

    return new PageImpl<>(res.getClickTransactionsList().stream()
        .map(dto -> mapper.map(dto, isAdmin ? ClickTransactionAdminResponse.class
            : ClickTransactionPublisherResponse.class))
        .collect(toList()), of(res.getNumber(), res.getSize()), res.getTotalElements());
  }
}

