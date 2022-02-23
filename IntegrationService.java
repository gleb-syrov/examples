package by.gsyrov.bamboolead.application.api.integration.service;

import static by.gsyrov.bamboolead.application.api.integration.utils.IntegrationUtils.convertToIntegrationInfoResponse;
import static by.gsyrov.bamboolead.application.api.integration.utils.IntegrationUtils.getIntegrationReq;
import static by.gsyrov.bamboolead.application.api.integration.utils.IntegrationUtils.getIntegrationUpdateReq;
import static by.gsyrov.bamboolead.application.api.integration.utils.IntegrationUtils.getResp;
import static by.gsyrov.bamboolead.integration.OfferType.ALL;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static org.hibernate.type.IntegerType.ZERO;
import static org.springframework.data.domain.PageRequest.of;
import static org.springframework.http.ResponseEntity.badRequest;
import static org.springframework.http.ResponseEntity.ok;

import by.gsyrov.bamboolead.application.api.integration.payload.request.IntegrationRequest;
import by.gsyrov.bamboolead.application.api.integration.payload.request.IntegrationRequestParams;
import by.gsyrov.bamboolead.application.api.integration.payload.response.IntegrationShortInfoResponse;
import by.gsyrov.bamboolead.application.api.offer.service.OfferService;
import by.gsyrov.bamboolead.application.api.user.service.SystemUserService;
import by.gsyrov.bamboolead.integration.IntegrationChangeStatusReq;
import by.gsyrov.bamboolead.integration.IntegrationInfoReq;
import by.gsyrov.bamboolead.integration.IntegrationPageRes;
import by.gsyrov.bamboolead.integration.IntegrationParamsReq;
import by.gsyrov.bamboolead.integration.IntegrationParamsReq.PageableReq;
import by.gsyrov.bamboolead.integration.IntegrationReq.IntegrationStatus;
import by.gsyrov.bamboolead.integration.IntegrationServiceGrpc.IntegrationServiceBlockingStub;
import by.gsyrov.bamboolead.integration.VoidReq;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class IntegrationService {

  private final ModelMapper mapper;
  private final OfferService offerService;
  private final SystemUserService systemUserService;

  @GrpcClient("IntegrationService")
  private IntegrationServiceBlockingStub stub;

  public ResponseEntity<?> create(final IntegrationRequest req) {
    return getResp(stub.create(getIntegrationReq(req)));
  }

  public ResponseEntity<?> update(final Long id, final IntegrationRequest req) {
    return getResp(stub.update(getIntegrationUpdateReq(id, req)));
  }

  public ResponseEntity<?> changeStatus(Long id, IntegrationStatus status) {
    return getResp(stub.changeStatus(IntegrationChangeStatusReq.newBuilder()
        .setId(id)
        .setStatus(status)
        .build())
    );
  }

  public ResponseEntity<?> getIntegration(final Long id) {
    try {
      return ok(convertToIntegrationInfoResponse(stub
          .getIntegration(IntegrationInfoReq.newBuilder().setId(id).build()))
      );
    } catch (Exception e) {
      return badRequest().body(e.getMessage());
    }
  }

  public Page<?> getAll(final IntegrationRequestParams param) {
    final IntegrationPageRes pageRes = stub.getAll(IntegrationParamsReq.newBuilder()
        .setStatus(isNull(param.getStatus()) ? ALL.name() : param.getStatus())
        .setOfferId(isNull(param.getOfferId()) ? ZERO.longValue() : param.getOfferId())
        .setPublisherId(isNull(param.getPublisherId()) ? ZERO.longValue() : param.getPublisherId())
        .setPageable(PageableReq.newBuilder()
            .setPage(param.getPage())
            .setSize(param.getSize())
            .setSort(param.getSort())
            .build())
        .build());
    final var pageable = pageRes.getPageable();
    final Map<Long, String> offerNameMap = offerService.getOfferNameMap(null);
    final Map<Long, String> publisherNameMap = systemUserService.getPublisherNameMap();

    return new PageImpl<>(pageRes.getIntegrationsList().stream()
        .map(i -> {
          final var res = mapper.map(i, IntegrationShortInfoResponse.class);
          res.setOfferName(offerNameMap.getOrDefault(i.getOfferId(), ALL.name()));
          res.setPublisherName(publisherNameMap.get(i.getPublisherId()));
          return res;
        })
        .collect(toList()),
        of(pageable.getNumber(), pageable.getSize()), pageable.getTotalElements());
  }

  public ResponseEntity<?> getClickPlaceholders() {
    return ok(stub.getClickPlaceholders(VoidReq.newBuilder().build()).getPlaceholdersMap());
  }

  public ResponseEntity<?> delete(final Long id) {
    return getResp(stub.delete(IntegrationInfoReq.newBuilder().setId(id).build()));
  }
}

