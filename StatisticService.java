package by.gsyrov.bamboolead.application.api.statistic.service;

import static by.gsyrov.bamboolead.application.api.statistic.utils.StatisticUtils.convertToClicksPerDayReq;
import static by.gsyrov.bamboolead.application.api.statistic.utils.StatisticUtils.convertToDailyClickStatisticsFilter;
import static by.gsyrov.bamboolead.application.api.statistic.utils.StatisticUtils.convertToDailyClickStatisticsFilterWithoutPageable;
import static by.gsyrov.bamboolead.application.api.statistic.utils.StatisticUtils.convertToGlobalStatisticReq;
import static by.gsyrov.bamboolead.application.api.statistic.utils.StatisticUtils.convertToUtmStatisticReq;
import static java.util.stream.Collectors.toList;
import static org.springframework.data.domain.PageRequest.of;
import static org.springframework.http.ResponseEntity.ok;

import by.gsyrov.bamboolead.application.api.offer.service.OfferService;
import by.gsyrov.bamboolead.application.api.statistic.payload.request.StatisticRequestParams;
import by.gsyrov.bamboolead.application.api.statistic.payload.response.ClickPerDayDto;
import by.gsyrov.bamboolead.application.api.statistic.payload.response.DailyClickStatisticDto;
import by.gsyrov.bamboolead.application.api.statistic.payload.response.DailyClickStatisticsTotalDto;
import by.gsyrov.bamboolead.application.api.statistic.payload.response.GlobalStatisticDto;
import by.gsyrov.bamboolead.application.api.statistic.payload.response.UtmStatisticDto;
import by.gsyrov.bamboolead.application.api.user.service.SystemUserService;
import by.gsyrov.bamboolead.statistic.ClicksPerDayReq;
import by.gsyrov.bamboolead.statistic.ClicksPerDayResp;
import by.gsyrov.bamboolead.statistic.DailyClickStatisticContainer;
import by.gsyrov.bamboolead.statistic.DailyClickStatisticTotalRes;
import by.gsyrov.bamboolead.statistic.DailyClickStatisticsFilter;
import by.gsyrov.bamboolead.statistic.GlobalStatisticReq;
import by.gsyrov.bamboolead.statistic.GlobalStatisticResp;
import by.gsyrov.bamboolead.statistic.StatisticServiceGrpc.StatisticServiceBlockingStub;
import by.gsyrov.bamboolead.statistic.StatisticServiceGrpc.StatisticServiceImplBase;
import by.gsyrov.bamboolead.statistic.UtmStatisticReq;
import by.gsyrov.bamboolead.statistic.UtmStatisticRes;
import io.grpc.stub.StreamObserver;
import java.util.List;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.client.inject.GrpcClient;
import net.devh.boot.grpc.server.service.GrpcService;
import org.modelmapper.ModelMapper;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.ResponseEntity;

/**
 * Statistic service gRPC.
 */
@GrpcService
@RequiredArgsConstructor
public class StatisticService extends StatisticServiceImplBase {

  private final ModelMapper mapper;
  private final OfferService offerService;
  private final SystemUserService systemUserService;

  @GrpcClient("StatisticService")
  private StatisticServiceBlockingStub stub;

  // This block is needed if the fronted will support gRPC.
  @Override
  public void getGlobalStatistic(GlobalStatisticReq request,
      StreamObserver<GlobalStatisticResp> responseObserver) {
    responseObserver.onNext(stub.getGlobalStatistic(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getClicksPerDay(ClicksPerDayReq request,
      StreamObserver<ClicksPerDayResp> responseObserver) {
    responseObserver.onNext(stub.getClicksPerDay(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getAllDailyStatistic(DailyClickStatisticsFilter request,
      StreamObserver<DailyClickStatisticContainer> responseObserver) {
    responseObserver.onNext(stub.getAllDailyStatistic(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getAllDailyStatisticTotal(DailyClickStatisticsFilter request,
      StreamObserver<DailyClickStatisticTotalRes> responseObserver) {
    responseObserver.onNext(stub.getAllDailyStatisticTotal(request));
    responseObserver.onCompleted();
  }

  @Override
  public void getUtmStatistic(UtmStatisticReq request,
      StreamObserver<UtmStatisticRes> responseObserver) {
    responseObserver.onNext(stub.getUtmStatistic(request));
    responseObserver.onCompleted();
  }

  /**
   * Method for get global statistic (REST).
   *
   * @param p {@link StatisticRequestParams} dto.
   * @return {@link GlobalStatisticDto} dto.
   */
  public ResponseEntity<GlobalStatisticDto> getGlobalStatisticRest(final StatisticRequestParams p) {
    return ok(mapper.map(stub.getGlobalStatistic(convertToGlobalStatisticReq(p)),
        GlobalStatisticDto.class));
  }

  /**
   * Method for get clicks per day (REST).
   *
   * @param params {@link StatisticRequestParams} dto.
   * @return {@link List} with {@link ClickPerDayDto} dto.
   */
  public ResponseEntity<List<ClickPerDayDto>> getClicksPerDayRest(
      final StatisticRequestParams params) {
    return ok(stub.getClicksPerDay(convertToClicksPerDayReq(params))
        .getClickPerDayListList().stream()
        .map(cpd -> mapper.map(cpd, ClickPerDayDto.class))
        .collect(toList()));
  }

  /**
   * Method for utm statistic (REST).
   *
   * @param params {@link StatisticRequestParams} dto.
   * @return {@link UtmStatisticDto} dto.
   */
  public Page<?> getUtmStatisticRest(final StatisticRequestParams params) {
    final UtmStatisticRes resp = stub.getUtmStatistic(convertToUtmStatisticReq(params));

    return new PageImpl<>(resp.getUtmStatisticListList().stream()
        .map(utm -> mapper.map(utm, UtmStatisticDto.class)).collect(toList()),
        of(resp.getPageable().getNumber(), resp.getPageable().getSize()),
        resp.getPageable().getTotalElements());
  }

  /**
   * Method for get daily click statistics (REST).
   *
   * @param params {@link StatisticRequestParams} dto.
   * @return {@link DailyClickStatisticDto} dto.
   */
  public Page<?> getDailyStatisticRest(final StatisticRequestParams params) {
    final var res = stub
        .getAllDailyStatistic(convertToDailyClickStatisticsFilter(params,
            offerService.getOfferNameMap(null),
            systemUserService.getPublisherNameMap()));

    return new PageImpl<>(res.getDailyClickStatisticList().stream()
        .map(dcs -> mapper.map(dcs, DailyClickStatisticDto.class)).collect(toList()),
        of(res.getPageable().getNumber(), res.getPageable().getSize()),
        res.getPageable().getTotalElements());
  }

  /**
   * Method for get daily click statistics total (REST).
   *
   * @param p {@link StatisticRequestParams} dto.
   * @return {@link DailyClickStatisticsTotalDto} dto.
   */
  public ResponseEntity<?> getDailyStatisticTotalRest(final StatisticRequestParams p) {
    return ok(mapper
        .map(stub.getAllDailyStatisticTotal(convertToDailyClickStatisticsFilterWithoutPageable(p)),
            DailyClickStatisticsTotalDto.class));
  }
}

