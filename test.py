from django.db import connection
from rest_framework.response import Response
from rest_framework import generics,status
from DialectAPI.models import SubmissionMapping, Timeout, UserStatus
from DialectAPI.utils import CustomPagination
from DialectAPI.serializers import UserStatusSerializer
from reports.serializers import (
    GeneralReportInput,
    GeneralReportResponse,
    ServiceLevelReportInput,
    UserStatusInput,
    TimeoutSerializer,
    WrapupReportInput,
    WrapupReport
)
from rest_framework.permissions import IsAuthenticated
import json

class GeneralReport(generics.ListAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = GeneralReportResponse
    pagination_class = CustomPagination
    def get_queryset(self):
        data = {
            'from_date' : self.request.query_params.get('from_date'),
            'to_date' : self.request.query_params.get('to_date'),
            'type' : self.request.query_params.get('type'),
            'sort_by' : self.request.query_params.get('sort_by'),
            'advisors' : self.request.query_params.get('advisors')
        }
        serializer = GeneralReportInput(data=data)
        if  serializer.is_valid():
            from_date_param = serializer.validated_data.get('from_date')
            to_date_param = serializer.validated_data.get('to_date')
            type_param = serializer.validated_data.get('type')
            sort_by = serializer.validated_data.get('sort_by')
            advisors = serializer.validated_data.get('advisors')

            if type_param == 'missed':
                self.serializer_class = TimeoutSerializer
                self.queryset = Timeout.objects.filter(
                    timeout_at__gte=from_date_param,
                    timeout_at__lte=to_date_param
                )
                if advisors != 'all':
                    advisors = advisors.strip('[]').split(',')
                    self.queryset = self.queryset.filter(user_id_id__in=advisors)

            if type_param == 'accepted':
                self.queryset = SubmissionMapping.objects.filter(
                    accept_time_frame__gte=from_date_param,
                    accept_time_frame__lte=to_date_param
                )

            if type_param == 'in-progress':
                self.queryset = SubmissionMapping.objects.filter(
                    start_on__gte=from_date_param,
                    start_on__lte=to_date_param
                )

            if type_param == 'completed':
                self.queryset = SubmissionMapping.objects.filter(
                    completed_on__gte=from_date_param,
                    completed_on__lte=to_date_param
                )

            if type_param == 'wrap-up':
                self.queryset = SubmissionMapping.objects.filter(
                    wrapup_on__gte=from_date_param,
                    wrapup_on__lte=to_date_param
                )

            if type_param == 'all':
                self.queryset = SubmissionMapping.objects.filter(
                    data_created_at__gte=from_date_param,
                    data_created_at__lte=to_date_param,
                    user_id__isnull=False
                )

            if type_param == 'queued':
                self.queryset = SubmissionMapping.objects.filter(
                    data_created_at__gte=from_date_param,
                    data_created_at__lte=to_date_param,
                    user_id__isnull=True
                )

            if advisors != 'all' and type_param not in ['missed', 'queued']:
                advisors = advisors.strip('[]').split(',')
                self.queryset = self.queryset.filter(user_id__in=advisors)
            return self.queryset.order_by(sort_by).all()
                    

class ServiceLevelReport(generics.RetrieveAPIView):
    permission_classes = [IsAuthenticated]
    def get(self, *args, **kwargs):
        data = {
            'from_date' : self.request.query_params.get('from_date'),
            'to_date' : self.request.query_params.get('to_date')
        }
        serializer = ServiceLevelReportInput(data=data)
        if not serializer.is_valid():
            return Response({
                "message":"Request parameters are invalid",
                "status": "ERROR",
                "data": {}
            }, status=status.HTTP_400_BAD_REQUEST)
        from_date = serializer.validated_data.get('from_date')
        to_date = self.request.query_params.get('to_date')
        accepted_submissions = SubmissionMapping.objects.filter(accept_time_frame__range=(from_date, to_date)).count()
        total_submissions = SubmissionMapping.objects.filter(data_created_at__range=(from_date, to_date)).count()
        missed_submissions = Timeout.objects.filter(timeout_at__range=(from_date, to_date)).count()
        inprogress_submissions = SubmissionMapping.objects.filter(start_on__range=(from_date, to_date)).count()
        completed_submissions = SubmissionMapping.objects.filter(wrapup_on__range=(from_date, to_date)).count()
        service_level_sql = """
            SELECT count(id)
            FROM public."DialectAPI_submissionmapping"
            WHERE EXTRACT(EPOCH FROM (accept_time_frame - data_created_at)) <= 600
            AND is_accepted = 1
            AND data_created_at >= %s
            AND data_created_at <= %s
        """
        handle_time_sql = """
            SELECT SUM(EXTRACT(EPOCH FROM (wrapup_on - accept_time_frame)))
            FROM public."DialectAPI_submissionmapping"
            WHERE submission_status = 4
            AND wrapup_on >= %s
            AND wrapup_on <= %s
        """
        params = [from_date, to_date]
        accepted_in_svl = 0
        overall_service_level = 0
        handle_time = 0
        average_handle_time = 0
        with connection.cursor() as cursor:
            cursor.execute(service_level_sql, params)
            row = cursor.fetchone()
            if row[0]:
                accepted_in_svl = row[0]
            cursor.execute(handle_time_sql, params)
            row = cursor.fetchone()
            if row[0]:
                handle_time = row[0]

        if completed_submissions != 0:
            average_handle_time = handle_time / completed_submissions

        if total_submissions != 0:
            # Client feedback: show count instead of percentage.
            # accepted_submissions = (accepted_submissions * 100) / total_submissions
            # missed_submissions = (missed_submissions * 100) / total_submissions
            # inprogress_submissions = (inprogress_submissions * 100) / total_submissions
            # completed_submissions = (completed_submissions * 100) / total_submissions
            overall_service_level = accepted_in_svl * 100 / total_submissions

        response_data = {
            "message":"Service Level Report",
            "status": "OK",
            'data': {
                'total_interactions':total_submissions,
                'overall_service_level': overall_service_level,
                'average_handle_time': average_handle_time,
                'status': {
                    'accepted': accepted_submissions,
                    'missed': missed_submissions,
                    'inprogress': inprogress_submissions,
                    'completed': completed_submissions
                }
            }
        }
        return Response(response_data, status=status.HTTP_200_OK)

class UserStatusReport(generics.RetrieveAPIView):
    permission_classes = [IsAuthenticated]
    serializer_class = UserStatusSerializer
    pagination_class = CustomPagination

    def get_queryset(self):
        queryset = UserStatus.objects.select_related('user_id').all()
        return queryset

    def get(self, *args, **kwargs):
        data = {
            'from_date' : self.request.query_params.get('from_date'),
            'to_date' : self.request.query_params.get('to_date'),
            'advisors' : self.request.query_params.get('advisors'), 
        }
        serializer = UserStatusInput(data=data)
        if not serializer.is_valid():
            return Response({
                "message":"Request parameters are invalid",
                "status": "ERROR",
                "data": {}
            }, status=status.HTTP_400_BAD_REQUEST)
        from_date = serializer.validated_data.get('from_date')
        to_date = self.request.query_params.get('to_date')
        advisors = self.request.query_params.get('advisors')
        total_submissions = 0
        completed_submissions = 0
        if advisors != 'all':
            advisors = advisors.strip('[]').split(',')
            total_submissions = SubmissionMapping.objects.filter(data_created_at__range=(from_date, to_date),user_id__in=advisors).count()

            completed_submissions = SubmissionMapping.objects.filter(wrapup_on__range=(from_date, to_date),user_id__in=advisors).count()
        else:
            total_submissions = SubmissionMapping.objects.filter(data_created_at__range=(from_date, to_date)).count()
            completed_submissions = SubmissionMapping.objects.filter(wrapup_on__range=(from_date, to_date)).count()

        service_level_sql = """
            SELECT count(id)
            FROM public."DialectAPI_submissionmapping"
            WHERE EXTRACT(EPOCH FROM (accept_time_frame - data_created_at)) <= 600
            AND is_accepted = 1
            AND data_created_at >= %s
            AND data_created_at <= %s
        """
        handle_time_sql = """
            SELECT SUM(EXTRACT(EPOCH FROM (wrapup_on - accept_time_frame)))
            FROM public."DialectAPI_submissionmapping"
            WHERE submission_status = 4
            AND wrapup_on >= %s
            AND wrapup_on <= %s
        """
        params = [from_date, to_date]

        if advisors != 'all':
            service_level_sql = service_level_sql + "AND user_id IN (" + ','.join(str(advisor) for advisor in advisors) + ")"
            handle_time_sql = handle_time_sql + "AND user_id IN (" + ','.join(str(advisor) for advisor in advisors) + ")"

        accepted_in_svl = 0
        service_level = 0
        handle_time = 0
        average_handle_time = 0
        with connection.cursor() as cursor:
            cursor.execute(service_level_sql, params)
            row = cursor.fetchone()
            if row[0]:
                accepted_in_svl = row[0]
            cursor.execute(handle_time_sql, params)
            row = cursor.fetchone()
            if row[0]:
                handle_time = row[0]
        if completed_submissions != 0:
            average_handle_time = handle_time / completed_submissions

        if total_submissions != 0:
            service_level = accepted_in_svl * 100 / total_submissions
        
        queryset = UserStatus.objects.filter(user_id__usermappingmaster__role_id=1).select_related('user_id').all()
        if advisors != 'all':
            queryset = UserStatus.objects.filter(user_id_id__in=advisors).select_related('user_id').all()

        page = self.paginate_queryset(queryset)
        if page is not None:
            self.serializer_class.Meta.depth = 1
            serialized_data = self.serializer_class(page, many=True).data
            response_data = {
            "message":"User Status Report",
            "status": "OK",
            'data': {
                'total_submission':total_submissions,
                'service_level': service_level,
                'average_handle_time': average_handle_time,
                'data': serialized_data
            }
        }
        return self.get_paginated_response(response_data)

class WrapupReport(generics.ListAPIView):
    pagination_class = CustomPagination
    permission_classes = [IsAuthenticated]
    serializer_class = WrapupReport

    def get_queryset(self):
        queryset = SubmissionMapping.objects.filter(
            submission_status = 4
        )
        data = {
            'from_date': self.request.query_params.get('from_date'),
            'to_date': self.request.query_params.get('to_date'),
            'sort_by': self.request.query_params.get('sort_by'),
            'advisors': self.request.query_params.get('advisors'),
            'wrapup_code': self.request.query_params.get('wrapup_code')
        }
        serializer = WrapupReportInput(data=data)
        serializer.is_valid()
        from_date = serializer.validated_data.get('from_date')
        to_date = serializer.validated_data.get('to_date')
        sort_by = serializer.validated_data.get('sort_by')
        advisors = serializer.validated_data.get('advisors')
        wrapup_code = serializer.validated_data.get('wrapup_code')
        queryset = queryset.filter(
            wrapup_on__gte = from_date,
            wrapup_on__lte = to_date,
            wrapup_code = wrapup_code
        )
        if advisors != 'all':
            advisors = advisors.strip('[]').split(',')
            queryset = queryset.filter(user_id__in=advisors)

        return queryset.select_related("user", "wrapup_code").order_by(sort_by)

    def list(self, request, *args, **kwargs):
        serializer = WrapupReportInput(data= {
            'from_date': self.request.query_params.get('from_date'),
            'to_date': self.request.query_params.get('to_date'),
            'sort_by': self.request.query_params.get('sort_by'),
            'advisors': self.request.query_params.get('advisors'),
            'wrapup_code': self.request.query_params.get('wrapup_code')
        })
        if  not serializer.is_valid():
            response_data = {
                "message": "Filter field validation failed",
                "status": "ERROR",
                'data': {},
                'error': json.loads(serializer.errors)
            }
            return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

        return super().list(request, *args, **kwargs)
